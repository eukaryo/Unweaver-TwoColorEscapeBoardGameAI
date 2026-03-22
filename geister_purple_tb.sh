#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${CXX:-}" ]]; then
  if command -v clang++ >/dev/null 2>&1; then
    CXX=clang++
  elif command -v clang++-20 >/dev/null 2>&1; then
    CXX=clang++-20
  else
    echo "error: clang++ not found (tried clang++ and clang++-20)" >&2
    exit 1
  fi
fi

SEEK_OBJ=./build/zstdseek_decompress.o
ZSTD_STATIC_LIB=./third_party/zstd/lib/libzstd.a

if [[ ! -f "${SEEK_OBJ}" || ! -f "${ZSTD_STATIC_LIB}" ]]; then
  echo "error: seekable-zstd dependencies not found" >&2
  echo "  expected: ${SEEK_OBJ}" >&2
  echo "  expected: ${ZSTD_STATIC_LIB}" >&2
  echo "hint: run ./prepare_seekable_zstd.sh first" >&2
  exit 1
fi

TEST_CXXFLAGS=(-std=c++20 -O3 -Wall -Wextra -Wpedantic -Wunused-variable -march=native -mtune=native -mbmi2 -mbmi)
BUILD_CXXFLAGS=(-std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Wunused-variable -march=native -mtune=native -mbmi2 -mbmi)
LDFLAGS=("${SEEK_OBJ}" "${ZSTD_STATIC_LIB}")

rm -f geister_purple_tb geister_blackbox_tests   geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm tablebase_io.pcm   geister_core.o geister_rank.o geister_rank_triplet.o tablebase_io.o geister_blackbox_tests.o geister_purple_tb.o

# Build & run blackbox tests (fail-fast)
"$CXX" "${TEST_CXXFLAGS[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
"$CXX" "${TEST_CXXFLAGS[@]}" -fprebuilt-module-path=. -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
"$CXX" "${TEST_CXXFLAGS[@]}" -fprebuilt-module-path=. -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm

"$CXX" "${TEST_CXXFLAGS[@]}" -c geister_core.cxx -o geister_core.o
"$CXX" "${TEST_CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_rank.cxx -o geister_rank.o
"$CXX" "${TEST_CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_rank_triplet.cxx -o geister_rank_triplet.o
"$CXX" "${TEST_CXXFLAGS[@]}" -fprebuilt-module-path=. -DGEISTER_ENABLE_TRIPLET_RANK_TESTS   -c geister_blackbox_tests.cpp -o geister_blackbox_tests.o
"$CXX" "${TEST_CXXFLAGS[@]}"   geister_core.o geister_rank.o geister_rank_triplet.o geister_blackbox_tests.o   -o geister_blackbox_tests
rm -f geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm   geister_core.o geister_rank.o geister_rank_triplet.o geister_blackbox_tests.o
./geister_blackbox_tests

# Build purple TB generator
"$CXX" "${BUILD_CXXFLAGS[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
"$CXX" "${BUILD_CXXFLAGS[@]}" -fprebuilt-module-path=. -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm
"$CXX" "${BUILD_CXXFLAGS[@]}" -fprebuilt-module-path=. -x c++-module --precompile tablebase_io.cxx -o tablebase_io.pcm

"$CXX" "${BUILD_CXXFLAGS[@]}" -c geister_core.cxx -o geister_core.o
"$CXX" "${BUILD_CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_rank_triplet.cxx -o geister_rank_triplet.o
"$CXX" "${BUILD_CXXFLAGS[@]}" -fprebuilt-module-path=. -c tablebase_io.cxx -o tablebase_io.o
"$CXX" "${BUILD_CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_purple_tb.cpp -o geister_purple_tb.o

"$CXX" "${BUILD_CXXFLAGS[@]}"   geister_core.o geister_rank_triplet.o tablebase_io.o geister_purple_tb.o   "${LDFLAGS[@]}"   -o geister_purple_tb

rm -f geister_core.pcm geister_rank_triplet.pcm tablebase_io.pcm   geister_core.o geister_rank_triplet.o tablebase_io.o geister_purple_tb.o
