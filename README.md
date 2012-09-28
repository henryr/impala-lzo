Impala-lzo
==========

This project provides support for reading LZO compressed tables from Impala.

Generally you should also install the Hadoop-lzo project which provides support for indexing the files produced by the lzop program.

Tables containing lzo compressed files must be created in Hive with:
  stored as
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'

To build the library do:
  You must set the environment variable IMPALA_HOME to the root of an Impala development
tree.
  cmake .
  make
at the top level will put the resulting libimpalalzo.so in the build directory.  This file should be moved to ${IMPALA_HOME}/lib/. OR any directory that is in the LD_LIBRARY_PATH of your running impalad servers.
