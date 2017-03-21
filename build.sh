#! /bin/bash
# (c) Copyright (2016) Cloudera, Inc.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

# This script is used to build the impala-lzo bits. It assumes that the second
# argument is the path to a checkout folder of the Impala code.

if [ $# -lt 2 ]; then
  echo "Usage $0 build_type impala_dir [toolchain_dir]"
  exit 1
fi

export IMPALA_HOME=$2
export IMPALA_LZO_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
export IMPALA_TOOLCHAIN=${3:-${IMPALA_HOME}/toolchain}

cd "$IMPALA_LZO_HOME"

# Make sure we have the impala env variables.
pushd ${IMPALA_HOME}
source ./bin/impala-config.sh
popd

# Regenerate CMake files to use the Impala toolchain
# Delete CMakeCache.txt because it doesn't save much time and can cause compile errors
# when changing branches.
rm -rf CMakeCache.txt CMakeFiles

MAKE_CMD=${MAKE_CMD:-make}
CMAKE_FLAGS=" -DCMAKE_BUILD_TYPE=$1"
if [[ "$MAKE_CMD" = "ninja" ]]; then
  CMAKE_FLAGS+=" -GNinja"
fi

cmake ${CMAKE_FLAGS} -DCMAKE_TOOLCHAIN_FILE=${IMPALA_HOME}/cmake_modules/toolchain.cmake
"${MAKE_CMD:-make}"
