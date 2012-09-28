// This file contains material form lzop-1.03
/* conf.h --

   This file is part of the lzop file compressor.

   Copyright (C) 1996-2010 Markus Franz Xaver Johannes Oberhumer
   All Rights Reserved.

   lzop and the LZO library are free software; you can redistribute them
   and/or modify them under the terms of the GNU General Public License as
   published by the Free Software Foundation; either version 2 of
   the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; see the file COPYING.
   If not, write to the Free Software Foundation, Inc.,
   59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

   Markus F.X.J. Oberhumer
   <markus@oberhumer.com>
   http://www.oberhumer.com/opensource/lzop/
 */

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>

#define ADLER32_INIT_VALUE  1
#define CRC32_INIT_VALUE    0

#define MIN_LZO_VERSION 0x0100
#define MIN_ZOP_VERSION 0x0900
#define LZOP_VERSION 0x1030

#define MAX_BLOCK_SIZE      (64*1024l*1024l)

/*************************************************************************
// lzop file header
**************************************************************************/

/* header flags */
#define F_ADLER32_D     0x00000001L
#define F_ADLER32_C     0x00000002L
#define F_STDIN         0x00000004L
#define F_STDOUT        0x00000008L
#define F_NAME_DEFAULT  0x00000010L
#define F_DOSISH        0x00000020L
#define F_H_EXTRA_FIELD 0x00000040L
#define F_H_GMTDIFF     0x00000080L
#define F_CRC32_D       0x00000100L
#define F_CRC32_C       0x00000200L
#define F_MULTIPART     0x00000400L
#define F_H_FILTER      0x00000800L
#define F_H_CRC32       0x00001000L
#define F_H_PATH        0x00002000L
#define F_MASK          0x00003FFFL

/* operating system & file system that created the file [mostly unused] */
#define F_OS_FAT        0x00000000L         /* DOS, OS2, Win95 */
#define F_OS_AMIGA      0x01000000L
#define F_OS_VMS        0x02000000L
#define F_OS_UNIX       0x03000000L
#define F_OS_VM_CMS     0x04000000L
#define F_OS_ATARI      0x05000000L
#define F_OS_OS2        0x06000000L         /* OS2 */
#define F_OS_MAC9       0x07000000L
#define F_OS_Z_SYSTEM   0x08000000L
#define F_OS_CPM        0x09000000L
#define F_OS_TOPS20     0x0a000000L
#define F_OS_NTFS       0x0b000000L         /* Win NT/2000/XP */
#define F_OS_QDOS       0x0c000000L
#define F_OS_ACORN      0x0d000000L
#define F_OS_VFAT       0x0e000000L         /* Win32 */
#define F_OS_MFS        0x0f000000L
#define F_OS_BEOS       0x10000000L
#define F_OS_TANDEM     0x11000000L
#define F_OS_SHIFT      24
#define F_OS_MASK       0xff000000L

/* character set for file name encoding [mostly unused] */
#define F_CS_NATIVE     0x00000000L
#define F_CS_LATIN1     0x00100000L
#define F_CS_DOS        0x00200000L
#define F_CS_WIN32      0x00300000L
#define F_CS_WIN16      0x00400000L
#define F_CS_UTF8       0x00500000L         /* filename is UTF-8 encoded */
#define F_CS_SHIFT      20
#define F_CS_MASK       0x00f00000L

/* these bits must be zero */
#define F_RESERVED      ((F_MASK | F_OS_MASK | F_CS_MASK) ^ 0xffffffffL)
