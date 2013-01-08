// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
// This file is based on code from the lzop program which is:
//   Copyright (C) 1996-2010 Markus Franz Xaver Johannes Oberhumer
//   All Rights Reserved.
//
//   lzop and the LZO library are free software; you can redistribute them
//   and/or modify them under the terms of the GNU General Public License as
//   published by the Free Software Foundation; either version 2 of
//   the License, or (at your option) any later version.
//
//   This program is distributed in the hope that it will be useful,
//   but WITHOUT ANY WARRANTY; without even the implied warranty of
//   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//   GNU General Public License for more details.
//
//   You should have received a copy of the GNU General Public License
//   along with this program; see the file COPYING.
//   If not, write to the Free Software Foundation, Inc.,
//   59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.


#include <hdfs.h>
#include <dlfcn.h>
#include <boost/algorithm/string.hpp>
#include "hdfs-lzo-text-scanner.h"
#include "exec/byte-stream.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scan-range-context.h"
#include "exec/serde-utils.h"
#include "exec/serde-utils.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"

#include "gen-cpp/Descriptors_types.h"

using namespace boost;
using namespace boost::algorithm;
using namespace impala;
using namespace std;

DEFINE_bool(disable_lzo_checksums, true,
    "Disable internal checksum checking for Lzo compressed files, defaults true");

// Suffix for index file: hdfs-filename.index
const string HdfsLzoTextScanner::INDEX_SUFFIX = ".index";

// The magic byte sequence at the beginning of an LZOP file.
static const uint8_t LZOP_MAGIC[9] =
    { 0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a };

extern "C" HdfsLzoTextScanner* CreateLzoTextScanner(
    HdfsScanNode* scan_node, RuntimeState* state) {
  return new HdfsLzoTextScanner(scan_node, state);
}

extern "C" void IssueInitialRanges(HdfsScanNode* scan_node,
    const vector<HdfsFileDesc*>& files) {
  HdfsLzoTextScanner::IssueInitialRanges(scan_node, files);
}

namespace impala {

HdfsLzoTextScanner::HdfsLzoTextScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : HdfsTextScanner(scan_node, state),
      block_buffer_pool_(new MemPool()),
      block_buffer_len_(0),
      bytes_remaining_(0),
      past_eosr_(false),
      eos_read_(false),
      only_parsing_header_(false),
      disable_checksum_(FLAGS_disable_lzo_checksums) {
}

HdfsLzoTextScanner::~HdfsLzoTextScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
    block_buffer_pool_->peak_allocated_bytes());
}

Status HdfsLzoTextScanner::Close() {
  if (!only_parsing_header_) {
    context_->AcquirePool(block_buffer_pool_.get());
  }
  only_parsing_header_ = false;
  context_->Flush();
  scan_node_->RangeComplete(THdfsFileFormat::LZO_TEXT, THdfsCompression::NONE);
  return Status::OK;
}

Status HdfsLzoTextScanner::ProcessScanRange(ScanRangeContext* context) {
  context_ = context;
  past_eosr_ = false;
  header_ = reinterpret_cast<LzoFileHeader*>(
      scan_node_->GetFileMetadata(context->filename()));
  if (header_ == NULL) {
    // This is the initial scan range just to parse the header
    only_parsing_header_ = true;
    header_ = state_->obj_pool()->Add(new LzoFileHeader());
    // Parse the header and read the index file.
    RETURN_IF_ERROR(ReadHeader());
    RETURN_IF_ERROR(ReadIndexFile());

    // Header is parsed, set the metadata in the scan node.
    scan_node_->SetFileMetadata(context->filename(), header_);
    return IssueFileRanges(context->filename());
  }

  if (context->scan_range()->offset() == 0) {
    Status status;
    SerDeUtils::SkipBytes(context, header_->header_size_, &status);
  } else {
    DCHECK(!header_->offsets.empty());
    RETURN_IF_ERROR(FindFirstBlock());
  }

  RETURN_IF_ERROR(HdfsTextScanner::ProcessScanRange(context));
  return Status::OK;
}

void HdfsLzoTextScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const vector<HdfsFileDesc*>& files) {
  // Issue just the header range for each file.  When the header is complete,
  // we'll issue the ranges for that file.  Read the minimum header size plus
  // up to 255 bytes of optional file name.
  for (int i = 0; i < files.size(); ++i) {
    // These files should be filtered by the planner.
    DCHECK(!ends_with(files[i]->filename, INDEX_SUFFIX));

    int64_t partition_id = reinterpret_cast<int64_t>(files[i]->ranges[0]->meta_data());
    DiskIoMgr::ScanRange* header_range = scan_node->AllocateScanRange(
        files[i]->filename.c_str(), HEADER_SIZE, 0, partition_id, -1);
    scan_node->AddDiskIoRange(header_range);
  }
}

Status HdfsLzoTextScanner::IssueFileRanges(const char* filename) {
  HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename);
  if (header_->offsets.empty()) {
    // If offsets is empty then there was on index file.  The file cannot be split.
    // If this contains the range starting at offset 0 generate a scan for whole file.
    const vector<DiskIoMgr::ScanRange*>& ranges = file_desc->ranges;
    for (int j = 0; j < ranges.size(); ++j) {
      if (ranges[j]->offset() != 0) {
        // Mark the other initial ranges complete
        scan_node_->RangeComplete(THdfsFileFormat::LZO_TEXT, THdfsCompression::NONE);
        continue;
      }
      int64_t partition_id = reinterpret_cast<int64_t>(file_desc->ranges[0]->meta_data());
      int64_t filesize;
      RETURN_IF_ERROR(GetFileSize(scan_node_->hdfs_connection(), filename, &filesize));
      DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(
          filename, filesize, 0, partition_id, -1);
      scan_node_->AddDiskIoRange(range);
    }
  } else {
    scan_node_->AddDiskIoRange(file_desc);
  }
  return Status::OK;
}

Status HdfsLzoTextScanner::ReadIndexFile() {
  string index_filename(context_->filename());
  index_filename.append(INDEX_SUFFIX);

  hdfsFS connection = scan_node_->hdfs_connection();

  // If there is no index file we can read the file by starting at the beginning
  // and reading through to the end.
  if (hdfsExists(connection, index_filename.c_str()) != 0) {
    LOG(WARNING) << "No index file for: " << context_->filename()
                 << ". Split scans are not possible.";
    return Status::OK;
  }

  hdfsFile index_file = hdfsOpenFile(connection,
       index_filename.c_str(), O_RDONLY, 0, 0, 0);

  if (index_file == NULL) {
    stringstream ss;
    ss << AppendHdfsErrorMessage("Error while opening index file: ", index_filename);
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  // TODO: This should go through the I/O manager.
  int read_size = 10 * 1024;
  uint8_t buffer[read_size];
  int num_read;

  while ((num_read = hdfsRead(connection, index_file, buffer, read_size)) > 0) {
    DCHECK_EQ(num_read % sizeof (int64_t), 0);
    for (uint8_t* bp = buffer; bp < buffer + num_read; bp += sizeof(int64_t)) {
      int64_t offset = SerDeUtils::GetLongInt(bp);
      header_->offsets.push_back(offset);
    }
  }

  int close_stat  = hdfsCloseFile(connection, index_file);

  if (num_read == -1) {
    stringstream ss;
    ss << AppendHdfsErrorMessage("Error while reading index file: ", index_filename);
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  if (close_stat == -1) {
    stringstream ss;
    ss << AppendHdfsErrorMessage("Error while closing index file: ", index_filename);
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  return Status::OK;
}

Status HdfsLzoTextScanner::FindFirstBlock() {
  int64_t offset = context_->file_offset();

  // Find the first block at or after the current file offset.  That way the
  // scan will start, or restart, on a block boundary.
  vector<int64_t>::iterator pos =
      upper_bound(header_->offsets.begin(), header_->offsets.end(), offset);

  if (pos == header_->offsets.end()) {
    stringstream ss;
    ss << "No block index for " << context_->filename() << " after offset: " << offset;
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  VLOG_ROW << "First Block: " << context_->filename()
           << " for " << offset << " @" << *pos;
  Status status;
  SerDeUtils::SkipBytes(context_, *pos - offset, &status);
  return status;
}

Status HdfsLzoTextScanner::ReadData() {
  do {
    Status status = ReadAndDecompressData();

    if (status.ok() || state_->abort_on_error()) return status;

    // On error try to skip forward to the next block.
    status = FindFirstBlock();
    if (!status.ok()) {
      if (state_->abort_on_error()) return status;

      // Just force to end of file, we cannot do more recovery if we can't find
      // the next block
      eos_read_ = true;
      bytes_remaining_ = 0;
      return Status::OK;
    }
  } while (!context_->eosr());

  // Reset the scanner state.
  HdfsTextScanner::ResetScanner();
  return Status::OK;
}

Status HdfsLzoTextScanner::FillByteBuffer(bool* eosr, int num_bytes) {
  *eosr = false;

  if (context_->eosr()) {
    // Set the read size to be the biggest a block could be. This needs
    // to be done here because the text scanner will set it to something
    // smaller during initialization.
    context_->set_read_past_buffer_size(MAX_BLOCK_COMPRESSED_SIZE);
    past_eosr_ = true;
    VLOG_ROW << "Reading past eosr: " << context_->filename()
             << " @" << context_->file_offset();
  }

  byte_buffer_read_size_ = 0;
  // Figure out if we have enough data and read more if necessary.
  if ((num_bytes == 0 && bytes_remaining_ == 0) || num_bytes > bytes_remaining_) {
    // Read and decompress the next block.
    RETURN_IF_ERROR(ReadData());
  }

  if (bytes_remaining_ != 0) {
    if (bytes_remaining_ >= num_bytes) {
      // We have enough bytes left to fill the request.
      byte_buffer_ptr_ = reinterpret_cast<char*>(block_buffer_ptr_);
      if (num_bytes == 0) {
         byte_buffer_read_size_ = bytes_remaining_;
      } else {
         byte_buffer_read_size_ = num_bytes;
      }
    } else {
      byte_buffer_ptr_ = reinterpret_cast<char*>(block_buffer_ptr_);
      byte_buffer_read_size_ = num_bytes;
    }
    // We assume a block is larger than the largest request.
    if (!eos_read_ && num_bytes > bytes_remaining_) {
      // Text only reads everything or 1024 so we do not need to handle this case.
      if (state_->LogHasSpace()) {
        state_->LogError("Unexpected read size in LZO decompressor");
      }
      DCHECK_LE(num_bytes, bytes_remaining_);
      return Status("Unexpected read size in LZO decompressor");
    }
  }

  byte_buffer_end_ = byte_buffer_ptr_ + byte_buffer_read_size_;
  if (bytes_remaining_ != 0) {
    bytes_remaining_ -= byte_buffer_read_size_;
    block_buffer_ptr_ += byte_buffer_read_size_;
  }

  *eosr = past_eosr_ || (eos_read_ && bytes_remaining_ == 0);

  if (VLOG_ROW_IS_ON && *eosr) {
    VLOG_ROW << "Returning eosr for: " << context_->filename()
             << " @" << context_->file_offset();
  }
  return Status::OK;
}

Status HdfsLzoTextScanner::Checksum(LzoChecksum type, const string& source,
    int expected_checksum, uint8_t* buffer, int length) {

  if (disable_checksum_) return Status::OK;

  // Do the checksum if requested.
  int32_t calculated_checksum;
  switch (type) {
    case CHECK_NONE:
      return Status::OK;

    case CHECK_CRC32:
      calculated_checksum = lzo_crc32(CRC32_INIT_VALUE, buffer, length);
      break;

    case CHECK_ADLER:
      calculated_checksum = lzo_adler32(ADLER32_INIT_VALUE, buffer, length);
      break;

    default:
      DCHECK(false);
  }

  if (calculated_checksum != expected_checksum) {
    stringstream ss;
    ss << "Checksum of " << source << " block failed on file: " << context_->filename()
       << " at offset: " << context_->file_offset() - length
       << " expected: " << expected_checksum << " got: " << calculated_checksum;
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }
  return Status::OK;
}

Status HdfsLzoTextScanner::ReadHeader() {
  uint8_t* magic;
  int num_read;
  bool eos;
  Status status;
  // Read the header in. HEADER_SIZE over estimates the maximum header.
  context_->GetBytes(&magic, HEADER_SIZE, &num_read, &eos, &status);
  RETURN_IF_ERROR(status);

  if (num_read < MIN_HEADER_SIZE) {
    stringstream ss;
    ss << "Read only " << num_read << " bytes from " << context_->filename();
    return Status(ss.str());
  }

  if (memcmp(magic, LZOP_MAGIC, sizeof(LZOP_MAGIC))) {
    stringstream ss;
    ss << "Invalid LZOP_MAGIC: '"
       << SerDeUtils::HexDump(magic, sizeof(LZOP_MAGIC)) << "'" << endl;
    status.AddErrorMsg(ss.str());
  } 

  uint8_t* header = magic + sizeof(LZOP_MAGIC);
  uint8_t* h_ptr = header;

  int version = SerDeUtils::GetSmallInt(h_ptr);
  if (version > LZOP_VERSION) {
    stringstream ss;
    ss << "Compressed with later version of lzop: " << version
       << " must be less than: " << LZOP_VERSION;
    status.AddErrorMsg(ss.str());
  }
  h_ptr += sizeof(int16_t);

  int libversion = SerDeUtils::GetSmallInt(h_ptr);
  if (libversion < MIN_LZO_VERSION) {
    stringstream ss;
    ss << "Compressed with incompatible lzo version: " << version
       << "must be at least: " << MIN_ZOP_VERSION;
    status.AddErrorMsg(ss.str());
  }
  h_ptr += sizeof(int16_t);

  // The version of LZOP needed to interpret this file.
  int neededversion = SerDeUtils::GetSmallInt(h_ptr);
  if (neededversion > LZOP_VERSION) {
    stringstream ss;
    ss << "Compressed with imp incompatible lzo version: " << neededversion
       << "must be at no more than: " << LZOP_VERSION;
    status.AddErrorMsg(ss.str());
  }
  h_ptr += sizeof(int16_t);

  uint8_t method = *h_ptr++;
  if (method < 1 || method > 3) {
    stringstream ss;
    ss << "Invalid compression method: " << method;
    status.AddErrorMsg(ss.str());
  }
  uint8_t level = *h_ptr++;

  int flags = SerDeUtils::GetInt(h_ptr);
  LzoChecksum header_checksum = (flags & F_H_CRC32) ? CHECK_CRC32 : CHECK_ADLER;
  header_->output_checksum_type_ = (flags & F_CRC32_D) ? CHECK_CRC32 :
      (flags & F_ADLER32_D) ? CHECK_ADLER : CHECK_NONE;
  header_->input_checksum_type_ = (flags & F_CRC32_C) ? CHECK_CRC32 :
      (flags & F_ADLER32_C) ? CHECK_ADLER : CHECK_NONE;
  
  if (flags & (F_RESERVED | F_MULTIPART | F_H_FILTER)) {
    stringstream ss;
    ss << "Unsupported flags: " << flags;
    status.AddErrorMsg(ss.str());
  }
  h_ptr += sizeof(int32_t);

  // skip mode and time fields
  h_ptr += 3 * sizeof(int32_t);

  // Skip filename.
  h_ptr += *h_ptr + 1;

  // The header always has a checksum.
  int32_t expected_checksum = SerDeUtils::GetInt(h_ptr);
  int32_t computed_checksum;
  if (header_checksum == CHECK_CRC32) {
    computed_checksum = CRC32_INIT_VALUE;
    computed_checksum = lzo_crc32(computed_checksum, header, h_ptr - header);
  } else {
    computed_checksum = ADLER32_INIT_VALUE;
    computed_checksum = lzo_adler32(computed_checksum, header, h_ptr - header);
  }

  if (computed_checksum != expected_checksum) {
    stringstream ss;
    ss << "Invalid header checksum: " << computed_checksum
       << " expected: " << expected_checksum;
    status.AddErrorMsg(ss.str());
  }
  h_ptr += sizeof(int32_t);

  // Skip the extra field if any.
  if (flags & F_H_EXTRA_FIELD) {
    int32_t len;
    Status status;
    SerDeUtils::ReadInt(context_, &len, &status);
    RETURN_IF_ERROR(status);
    // Add the size of the len and the checksum and the len to the total h_ptr size.
    h_ptr += (2 * sizeof(int32_t)) + len;
  }

  VLOG_FILE << "Reading: " << context_->filename() << " Header: version: " << version 
            << "(" << libversion << "/" << neededversion << ")"
            << " method: " << (int)method << "@" << (int)level
            << " flags: " << flags;
  if (!status.ok()) {
    stringstream ss;
    ss << "Invalid header information: " << context_->filename();
    status.AddErrorMsg(ss.str());
    return status;
  }

  header_->header_size_ = h_ptr - magic;

  return Status::OK;
}

Status HdfsLzoTextScanner::ReadAndDecompressData() {
  bytes_remaining_ = 0;
  uint8_t* input_buffer;
  int bytes_read;
  Status status;
  // Read the uncompressed, compressed lengths
  context_->GetBytes(&input_buffer,
      2 * sizeof(int32_t), &bytes_read, &eos_read_, &status);
  RETURN_IF_ERROR(status);
  if (bytes_read < 2 * sizeof(int32_t)) {
    if (eos_read_) return Status::OK;
    return Status("GetBytes returned too little data");
  }
  DCHECK_EQ(bytes_read, 2 * sizeof(int32_t));

  int32_t out_length = SerDeUtils::GetInt(input_buffer);
  input_buffer += sizeof(int32_t);
  if (out_length == 0) {
    eos_read_ = true;
    return Status::OK;
  }

  // Length of the compressed data.
  int32_t in_length = SerDeUtils::GetInt(input_buffer);

  if (in_length > MAX_BLOCK_SIZE) {
    stringstream ss;
    ss << "Blocksize: " << in_length << " is greater than MAX_BLOCK_SIZE: "
       << MAX_BLOCK_SIZE;
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  int out_checksum;
  // The checksum of the uncompressed data.
  if (header_->output_checksum_type_ != CHECK_NONE) {
    SerDeUtils::ReadInt(context_, &out_checksum, &status);
    RETURN_IF_ERROR(status);
  }
  
  // If the compressed data size was >= than the uncompressed data size then
  // the uncompressed data is stored and there is no compressed checksum.
  int in_checksum = 0;
  if (in_length < out_length && header_->input_checksum_type_ != CHECK_NONE) {
    SerDeUtils::ReadInt(context_, &in_checksum, &status);
    RETURN_IF_ERROR(status);
  } else {
    in_checksum = out_checksum;
  }

  // Read in the compressed data
  context_->GetBytes(&input_buffer, in_length, &bytes_read, &eos_read_, &status);
  RETURN_IF_ERROR(status);

  // Checksum the data.
  RETURN_IF_ERROR(Checksum(header_->input_checksum_type_,
      "compressed", in_checksum, input_buffer, in_length));

  // If the data did not compress we are done.
  if (in_length == out_length) {
    block_buffer_ptr_ = input_buffer;
    bytes_remaining_ = in_length;
    return Status::OK;
  }

  if (!context_->compact_data()) {
    context_->AcquirePool(block_buffer_pool_.get());
    block_buffer_len_ = 0;
  }
  if (out_length > block_buffer_len_) {
    block_buffer_ = block_buffer_pool_->Allocate(out_length);
    block_buffer_len_ = out_length;
  }
  block_buffer_ptr_ = block_buffer_;
  bytes_remaining_ = out_length;

  // Decompress the data.  lzop always uses lzo1x.
  int ret = lzo1x_decompress_safe(input_buffer,
      in_length, block_buffer_, reinterpret_cast<lzo_uint*>(&out_length), NULL);

  if (ret != LZO_E_OK || bytes_remaining_ != out_length) {
    stringstream ss;
    ss << "Decompression failed on file: " << context_->filename()
       << " at offset: " << context_->file_offset() << " returned: " << ret
       << " output size: " << out_length << "expected: " << block_buffer_len_;
      state_->LogError(ss.str());
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }

  // Do the checksum if requested.
  RETURN_IF_ERROR(Checksum(header_->output_checksum_type_,
     "decompressed", out_checksum, block_buffer_, out_length));

  // Return end of scan range even if there are bytes in the disk buffer.
  // We fetched the next disk buffer past EOSR to complete the read of this compressed
  // block.  When the scanner finishes with the data we return here it must
  // go into Finish mode and complete its final row.
  eos_read_ = context_->eosr();
  VLOG_ROW << "LZO decompressed " << out_length << " bytes from " << context_->filename()
           << " @" << context_->file_offset() - in_length;

  return Status::OK;
}
}
