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

CXXFLAGS=(-std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Wunused-variable -march=native -mtune=native -mbmi2 -mbmi)
LDFLAGS=(-lzstd)

rm -f geister_purple_tb_red2_repack   geister_core.pcm geister_rank_triplet.pcm   geister_core.o geister_rank_triplet.o geister_purple_tb_red2_repack.o

"$CXX" "${CXXFLAGS[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
"$CXX" "${CXXFLAGS[@]}" -fprebuilt-module-path=. -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm

"$CXX" "${CXXFLAGS[@]}" -c geister_core.cxx -o geister_core.o
"$CXX" "${CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_rank_triplet.cxx -o geister_rank_triplet.o
"$CXX" "${CXXFLAGS[@]}" -fprebuilt-module-path=. -c geister_purple_tb_red2_repack.cpp -o geister_purple_tb_red2_repack.o

"$CXX" "${CXXFLAGS[@]}"   geister_core.o geister_rank_triplet.o geister_purple_tb_red2_repack.o   "${LDFLAGS[@]}" -o geister_purple_tb_red2_repack

rm -f geister_core.pcm geister_rank_triplet.pcm   geister_core.o geister_rank_triplet.o geister_purple_tb_red2_repack.o

./geister_purple_tb_red2_repack --self-test-only
