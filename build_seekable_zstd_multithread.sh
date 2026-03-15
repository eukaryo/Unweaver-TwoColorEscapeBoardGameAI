#!/usr/bin/env bash
set -euo pipefail

rm -f seekable_zstd_multithread

CXXFLAGS="-std=c++20 -O3 -DNDEBUG -march=native -mtune=native -flto -pthread"

clang++-20 $CXXFLAGS seekable_zstd_multithread.cpp -lzstd -o seekable_zstd_multithread