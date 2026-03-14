#!/usr/bin/env bash
set -euo pipefail

TARGET='all'
CXX_OVERRIDE=''
OMPFLAGS_OVERRIDE=''

usage() {
  cat <<'USAGE'
Usage: ./build_public.sh [options] [target]

Targets:
  runtime    Build geister_stdio_baseline_player
  builders   Build perfect-information tablebase builders
  tests      Build geister_blackbox_tests
  all        Build everything above (default)
  clean      Remove build artifacts produced by this script

Options:
  --cxx PATH           C++ compiler to use (auto-detect clang++ if omitted)
  --omp-flags "..."    OpenMP flags for builder compilation
  -h, --help           Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    runtime|builders|tests|all|clean)
      TARGET="$1"
      ;;
    --cxx)
      shift
      [[ $# -gt 0 ]] || { echo 'Error: --cxx requires an argument' >&2; exit 1; }
      CXX_OVERRIDE="$1"
      ;;
    --omp-flags)
      shift
      [[ $# -gt 0 ]] || { echo 'Error: --omp-flags requires an argument' >&2; exit 1; }
      OMPFLAGS_OVERRIDE="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option or target: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

detect_cxx() {
  if [[ -n "${CXX_OVERRIDE}" ]]; then
    printf '%s\n' "${CXX_OVERRIDE}"
    return 0
  fi

  local cxx
  for cxx in clang++ clang++-20 clang++-19 clang++-18 clang++-17 clang++-16; do
    if command -v "$cxx" >/dev/null 2>&1; then
      command -v "$cxx"
      return 0
    fi
  done
  return 1
}

CXX="$(detect_cxx || true)"
if [[ -z "${CXX}" ]]; then
  echo 'Error: clang++ not found (tried clang++ and clang++-NN)' >&2
  exit 1
fi

detect_ompflags() {
  if [[ -n "${OMPFLAGS_OVERRIDE}" ]]; then
    printf '%s\n' "${OMPFLAGS_OVERRIDE}"
    return 0
  fi

  local tmp_cpp=.build_public_omp_test.cpp
  local tmp_bin=.build_public_omp_test
  cat > "${tmp_cpp}" <<'CPP'
#include <omp.h>
#include <cstdio>
int main() {
  int n = 0;
  #pragma omp parallel reduction(+:n)
  n += 1;
  std::printf("%d\n", n);
  return 0;
}
CPP

  local base ver=''
  base="$(basename "${CXX}")"
  if [[ "${base}" =~ ([0-9]+)$ ]]; then
    ver="${BASH_REMATCH[1]}"
  fi

  local candidates=()
  candidates+=("-fopenmp")
  candidates+=("-fopenmp=libomp")

  if [[ -n "${ver}" && -d "/usr/lib/llvm-${ver}/lib" ]]; then
    candidates+=("-fopenmp -L/usr/lib/llvm-${ver}/lib -Wl,-rpath,/usr/lib/llvm-${ver}/lib")
    candidates+=("-fopenmp=libomp -L/usr/lib/llvm-${ver}/lib -Wl,-rpath,/usr/lib/llvm-${ver}/lib")
  fi

  if [[ -d /usr/lib/x86_64-linux-gnu ]]; then
    candidates+=("-fopenmp -L/usr/lib/x86_64-linux-gnu")
    candidates+=("-fopenmp=libomp -L/usr/lib/x86_64-linux-gnu")
  fi

  local flags
  for flags in "${candidates[@]}"; do
    if ${CXX} -std=c++20 ${flags} "${tmp_cpp}" -o "${tmp_bin}" >/dev/null 2>&1; then
      rm -f "${tmp_cpp}" "${tmp_bin}"
      printf '%s\n' "${flags}"
      return 0
    fi
  done

  rm -f "${tmp_cpp}" "${tmp_bin}"
  return 1
}

SEEK_OBJ=./build/zstdseek_decompress.o
ZSTD_STATIC_LIB=./third_party/zstd/lib/libzstd.a

ensure_seekable_zstd() {
  if [[ ! -f "${SEEK_OBJ}" || ! -f "${ZSTD_STATIC_LIB}" ]]; then
    echo 'Error: seekable-zstd dependencies not found.' >&2
    echo "  expected: ${SEEK_OBJ}" >&2
    echo "  expected: ${ZSTD_STATIC_LIB}" >&2
    echo 'Hint: run ./prepare_seekable_zstd.sh first.' >&2
    exit 1
  fi
}

clean_runtime_artifacts() {
  rm -f geister_stdio_baseline_player geister_stdio_baseline_player.o \
    geister_core.pcm geister_interface.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_tb_handler.pcm geister_random_player.pcm confident_player.pcm \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o confident_player.o
}

clean_test_artifacts() {
  rm -f geister_blackbox_tests geister_blackbox_tests.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o
}

clean_builder_artifacts() {
  rm -f geister_perfect_information_tb \
    geister_perfect_information_tb_9_10 geister_perfect_information_tb_9_10_repack_obsblk \
    geister_perfect_information_tb.o \
    geister_perfect_information_tb_9_10.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o
}

clean_all_artifacts() {
  clean_runtime_artifacts
  clean_test_artifacts
  clean_builder_artifacts
  rm -f .build_public_omp_test.cpp .build_public_omp_test
}

build_runtime() {
  ensure_seekable_zstd
  clean_runtime_artifacts

  CXXFLAGS='-std=c++20 -O3 -DNDEBUG -march=native -mtune=native -mbmi2 -mbmi -flto -pthread'
  MPATH='-fprebuilt-module-path=.'

  # 1) Precompile module interfaces -> .pcm (BMI)
  ${CXX} ${CXXFLAGS} -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_interface.cxx -o geister_interface.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank_obsblk.cxx -o geister_rank_obsblk.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile tablebase_io.cxx -o tablebase_io.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_tb_handler.cxx -o geister_tb_handler.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_random_player.cxx -o geister_random_player.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile confident_player.cxx -o confident_player.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  ${CXX} ${CXXFLAGS} -c geister_core.cxx -o geister_core.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_interface.cxx -o geister_interface.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank.cxx -o geister_rank.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank_triplet.cxx -o geister_rank_triplet.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank_obsblk.cxx -o geister_rank_obsblk.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c tablebase_io.cxx -o tablebase_io.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_tb_handler.cxx -o geister_tb_handler.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_random_player.cxx -o geister_random_player.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c confident_player.cxx -o confident_player.o

  # 3) Compile the stdio baseline player (imports modules via BMIs)
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_stdio_baseline_player.cpp -o geister_stdio_baseline_player.o

  # 4) Link
  ${CXX} ${CXXFLAGS} \
    geister_stdio_baseline_player.o \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o confident_player.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_stdio_baseline_player

  rm -f \
    geister_stdio_baseline_player.o \
    geister_core.pcm geister_interface.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_tb_handler.pcm geister_random_player.pcm confident_player.pcm \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o confident_player.o
}

build_tests() {
  clean_test_artifacts

  CXXFLAGS='-std=c++20 -O3 -march=native -mtune=native -flto'
  MPATH='-fprebuilt-module-path=.'

  # 1) Precompile module interfaces -> .pcm (BMI)
  ${CXX} ${CXXFLAGS} -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  ${CXX} ${CXXFLAGS} -c geister_core.cxx -o geister_core.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank.cxx -o geister_rank.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank_triplet.cxx -o geister_rank_triplet.o

  # 3) Compile the blackbox tests (imports modules via BMIs)
  ${CXX} ${CXXFLAGS} ${MPATH} -DGEISTER_ENABLE_TRIPLET_RANK_TESTS -c geister_blackbox_tests.cpp -o geister_blackbox_tests.o

  # 4) Link
  ${CXX} ${CXXFLAGS} \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_blackbox_tests.o \
    -o geister_blackbox_tests

  rm -f \
    geister_blackbox_tests.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o
}

build_builders() {
  ensure_seekable_zstd
  clean_builder_artifacts

  OMPFLAGS="$(detect_ompflags || true)"
  if [[ -z "${OMPFLAGS}" ]]; then
    echo 'Error: failed to auto-detect OpenMP flags for builders.' >&2
    echo 'Retry with: ./build_public.sh --omp-flags "..." builders' >&2
    exit 1
  fi

  CXXFLAGS="-std=c++20 -O3 -DNDEBUG -march=native -mtune=native -mbmi2 -mbmi -flto ${OMPFLAGS}"
  MPATH='-fprebuilt-module-path=.'

  # 1) Precompile module interfaces -> .pcm (BMI)
  ${CXX} ${CXXFLAGS} -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile geister_rank_obsblk.cxx -o geister_rank_obsblk.pcm
  ${CXX} ${CXXFLAGS} ${MPATH} -x c++-module --precompile tablebase_io.cxx -o tablebase_io.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  ${CXX} ${CXXFLAGS} -c geister_core.cxx -o geister_core.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank.cxx -o geister_rank.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank_triplet.cxx -o geister_rank_triplet.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_rank_obsblk.cxx -o geister_rank_obsblk.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c tablebase_io.cxx -o tablebase_io.o

  # 3) Compile builder translation units (imports modules via BMIs)
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_perfect_information_tb.cpp -o geister_perfect_information_tb.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_perfect_information_tb_9_10.cpp -o geister_perfect_information_tb_9_10.o
  ${CXX} ${CXXFLAGS} ${MPATH} -c geister_perfect_information_tb_9_10_repack_obsblk.cpp -o geister_perfect_information_tb_9_10_repack_obsblk.o

  # 4) Link
  ${CXX} ${CXXFLAGS} \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb

  ${CXX} ${CXXFLAGS} \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb_9_10.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb_9_10

  ${CXX} ${CXXFLAGS} \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb_9_10_repack_obsblk

  rm -f \
    geister_perfect_information_tb.o \
    geister_perfect_information_tb_9_10.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o
}

case "${TARGET}" in
  runtime)
    build_runtime
    ;;
  builders)
    build_builders
    ;;
  tests)
    build_tests
    ;;
  all)
    build_tests
    build_runtime
    build_builders
    ;;
  clean)
    clean_all_artifacts
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
