#! /bin/bash
# (c) Copyright (2016) Cloudera, Inc.

set -ex

# this script is used to build the impala-lzo bits it assumes that the first
# argument is the path to a checkout folder of the Impala code.

if [ $# -lt 1 ]; then
    echo Usage build.sh [impala dir] \(toolchain\)
    echo
    exit 1
fi

export IMPALA_HOME=$1
export IMPALA_LZO_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
export IMPALA_TOOLCHAIN=${2:-${IMPALA_HOME}/toolchain}

# Make sure we have the impala env variables
pushd ${IMPALA_HOME}
source ./bin/impala-config.sh
./bin/gen_build_version.py
popd

# Regenerate CMake files to use the Impala toolchain
cmake -DCMAKE_TOOLCHAIN_FILE=${IMPALA_HOME}/cmake_modules/toolchain.cmake

#Build
make -j$(nproc)
