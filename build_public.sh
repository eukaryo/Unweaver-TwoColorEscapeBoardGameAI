#!/usr/bin/env bash
set -euo pipefail

TARGET='all'
CXX_OVERRIDE=''
OMPFLAGS_OVERRIDE=''
BUILD_MODE='native'     # default: optimize for the current machine
LTO_MODE='auto'         # auto | on | off

CXX=''
CXX_PATH_PREFIX=''
ARCH_FLAGS=()
LTO_COMPILE_FLAGS=()
LTO_LINK_FLAGS=()
OMP_COMPILE_FLAGS=()
OMP_LINK_FLAGS=()

usage() {
  cat <<'USAGE'
Usage: ./build_public.sh [options] [target]

Targets:
  runtime    Build geister_stdio_baseline_player
  builders   Build perfect-information and purple tablebase builders
  tests      Build geister_blackbox_tests
  all        Build everything above (default)
  clean      Remove build artifacts produced by this script

Options:
  --cxx PATH           C++ compiler to use (auto-detect clang++ if omitted)
  --omp-flags "..."    OpenMP flags override for builders
  --native             Build host-optimized binaries (default)
  --portable           Drop -march/-mtune=native while keeping BMI2 enabled
  --lto                Require LTO; fail if unavailable
  --no-lto             Disable LTO
  -h, --help           Show this help

Notes:
  - By default the script emits the fastest binaries for the current machine:
    -march=native -mtune=native -mbmi2 -mbmi
  - The code currently requires BMI2 support; --portable keeps -mbmi2/-mbmi.
  - LTO is auto-detected by default. The script tries lld first when available
    and falls back to a non-LTO build if the local toolchain cannot link -flto
    objects.
  - Linker-only flags such as -fuse-ld=lld and -Wl,... are passed only during
    actual link steps, so clang does not emit
    "argument unused during compilation" warnings during .o/.pcm generation.
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
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
    --portable)
      BUILD_MODE='portable'
      ;;
    --native)
      BUILD_MODE='native'
      ;;
    --lto)
      LTO_MODE='on'
      ;;
    --no-lto)
      LTO_MODE='off'
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

have() {
  command -v "$1" >/dev/null 2>&1
}

run_cxx() {
  if [[ -n "${CXX_PATH_PREFIX}" ]]; then
    PATH="${CXX_PATH_PREFIX}:${PATH}" "${CXX}" "$@"
  else
    "${CXX}" "$@"
  fi
}

detect_lld_bindir() {
  local cxx_dir base ver candidate

  cxx_dir="$(dirname "${CXX}")"
  if [[ -x "${cxx_dir}/ld.lld" ]]; then
    printf '%s\n' "${cxx_dir}"
    return 0
  fi

  if have ld.lld; then
    dirname "$(command -v ld.lld)"
    return 0
  fi

  base="$(basename "${CXX}")"
  if [[ "${base}" =~ ([0-9]+)$ ]]; then
    ver="${BASH_REMATCH[1]}"
    candidate="/usr/lib/llvm-${ver}/bin/ld.lld"
    if [[ -x "${candidate}" ]]; then
      dirname "${candidate}"
      return 0
    fi
  fi

  return 1
}

test_linker_flags() {
  local tmp_cpp=.build_public_lto_test.cpp
  local tmp_bin=.build_public_lto_test

  cat > "${tmp_cpp}" <<'CPP'
int main() { return 0; }
CPP

  if run_cxx -std=c++20 -O2 "$@" "${tmp_cpp}" -o "${tmp_bin}" >/dev/null 2>&1; then
    rm -f "${tmp_cpp}" "${tmp_bin}"
    return 0
  fi

  rm -f "${tmp_cpp}" "${tmp_bin}"
  return 1
}

configure_arch_flags() {
  ARCH_FLAGS=(-mbmi2 -mbmi)
  if [[ "${BUILD_MODE}" == 'native' ]]; then
    ARCH_FLAGS=(-march=native -mtune=native "${ARCH_FLAGS[@]}")
  fi
}

configure_lto_flags() {
  local lld_bindir=''
  local tried_lld=0

  LTO_COMPILE_FLAGS=()
  LTO_LINK_FLAGS=()
  CXX_PATH_PREFIX=''

  case "${LTO_MODE}" in
    off)
      log 'LTO: disabled (--no-lto)'
      return 0
      ;;
    on|auto)
      ;;
    *)
      echo "Error: internal invalid LTO mode: ${LTO_MODE}" >&2
      exit 1
      ;;
  esac

  lld_bindir="$(detect_lld_bindir || true)"
  if [[ -n "${lld_bindir}" ]]; then
    tried_lld=1
    CXX_PATH_PREFIX="${lld_bindir}"
    if test_linker_flags -flto -fuse-ld=lld; then
      LTO_COMPILE_FLAGS=(-flto)
      LTO_LINK_FLAGS=(-flto -fuse-ld=lld)
      log "LTO: enabled with lld (${lld_bindir}/ld.lld)"
      return 0
    fi
    CXX_PATH_PREFIX=''
  fi

  if test_linker_flags -flto; then
    LTO_COMPILE_FLAGS=(-flto)
    LTO_LINK_FLAGS=(-flto)
    log 'LTO: enabled'
    return 0
  fi

  CXX_PATH_PREFIX=''
  LTO_COMPILE_FLAGS=()
  LTO_LINK_FLAGS=()

  if [[ "${LTO_MODE}" == 'on' ]]; then
    if (( tried_lld )); then
      echo 'Error: LTO was requested, but neither -flto -fuse-ld=lld nor plain -flto linked successfully.' >&2
    else
      echo 'Error: LTO was requested, but plain -flto did not link successfully.' >&2
    fi
    echo 'Hint: retry with --no-lto or install a working lld/LLVM LTO toolchain.' >&2
    exit 1
  fi

  log 'LTO: unavailable on this toolchain; continuing without -flto'
}

split_omp_override_flags() {
  local override="$1"
  local -a raw=()
  local -a compile=()
  local -a link=()

  read -r -a raw <<<"${override}"
  if [[ ${#raw[@]} -eq 0 ]]; then
    OMP_COMPILE_FLAGS=()
    OMP_LINK_FLAGS=()
    return 0
  fi

  local i token
  for (( i = 0; i < ${#raw[@]}; ++i )); do
    token="${raw[$i]}"
    case "${token}" in
      -Wl,*|-L*|-l*)
        link+=("${token}")
        ;;
      -Xlinker)
        link+=("${token}")
        if (( i + 1 < ${#raw[@]} )); then
          ((++i))
          link+=("${raw[$i]}")
        fi
        ;;
      *)
        compile+=("${token}")
        link+=("${token}")
        ;;
    esac
  done

  OMP_COMPILE_FLAGS=("${compile[@]}")
  OMP_LINK_FLAGS=("${link[@]}")
}

test_omp_flag_pair() {
  local compile_flags_str="$1"
  local link_flags_str="$2"
  local -a compile_flags=()
  local -a link_flags=()
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

  if [[ -n "${compile_flags_str}" ]]; then
    read -r -a compile_flags <<<"${compile_flags_str}"
  fi
  if [[ -n "${link_flags_str}" ]]; then
    read -r -a link_flags <<<"${link_flags_str}"
  fi

  if run_cxx -std=c++20 "${compile_flags[@]}" "${tmp_cpp}" "${link_flags[@]}" -o "${tmp_bin}" >/dev/null 2>&1; then
    rm -f "${tmp_cpp}" "${tmp_bin}"
    return 0
  fi

  rm -f "${tmp_cpp}" "${tmp_bin}"
  return 1
}

detect_ompflags() {
  OMP_COMPILE_FLAGS=()
  OMP_LINK_FLAGS=()

  if [[ -n "${OMPFLAGS_OVERRIDE}" ]]; then
    split_omp_override_flags "${OMPFLAGS_OVERRIDE}"
    return 0
  fi

  local base ver=''
  base="$(basename "${CXX}")"
  if [[ "${base}" =~ ([0-9]+)$ ]]; then
    ver="${BASH_REMATCH[1]}"
  fi

  local -a compile_candidates=()
  local -a link_candidates=()

  compile_candidates+=("-fopenmp")
  link_candidates+=("-fopenmp")

  compile_candidates+=("-fopenmp=libomp")
  link_candidates+=("-fopenmp=libomp")

  if [[ -n "${ver}" && -d "/usr/lib/llvm-${ver}/lib" ]]; then
    compile_candidates+=("-fopenmp")
    link_candidates+=("-fopenmp -L/usr/lib/llvm-${ver}/lib -Wl,-rpath,/usr/lib/llvm-${ver}/lib")

    compile_candidates+=("-fopenmp=libomp")
    link_candidates+=("-fopenmp=libomp -L/usr/lib/llvm-${ver}/lib -Wl,-rpath,/usr/lib/llvm-${ver}/lib")
  fi

  if [[ -d /usr/lib/x86_64-linux-gnu ]]; then
    compile_candidates+=("-fopenmp")
    link_candidates+=("-fopenmp -L/usr/lib/x86_64-linux-gnu")

    compile_candidates+=("-fopenmp=libomp")
    link_candidates+=("-fopenmp=libomp -L/usr/lib/x86_64-linux-gnu")
  fi

  local i
  for (( i = 0; i < ${#compile_candidates[@]}; ++i )); do
    if test_omp_flag_pair "${compile_candidates[$i]}" "${link_candidates[$i]}"; then
      read -r -a OMP_COMPILE_FLAGS <<<"${compile_candidates[$i]}"
      read -r -a OMP_LINK_FLAGS <<<"${link_candidates[$i]}"
      return 0
    fi
  done

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
    geister_tb_handler.pcm geister_random_player.pcm geister_proven_escape.pcm geister_purple_winning.pcm confident_player.pcm \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o geister_proven_escape.o geister_purple_winning.o confident_player.o
}

clean_test_artifacts() {
  rm -f geister_blackbox_tests geister_blackbox_tests.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o
}

clean_builder_artifacts() {
  rm -f geister_perfect_information_tb geister_purple_tb \
    geister_perfect_information_tb_9_10 geister_perfect_information_tb_9_10_repack_obsblk \
    geister_purple_tb_red2 geister_purple_tb_red2_repack \
    geister_perfect_information_tb.o geister_purple_tb.o \
    geister_perfect_information_tb_9_10.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    geister_purple_tb_red2.o geister_purple_tb_red2_repack.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o
}

clean_all_artifacts() {
  clean_runtime_artifacts
  clean_test_artifacts
  clean_builder_artifacts
  rm -f .build_public_omp_test.cpp .build_public_omp_test \
    .build_public_lto_test.cpp .build_public_lto_test
}

build_runtime() {
  ensure_seekable_zstd
  clean_runtime_artifacts

  local -a warnflags=(-Wall -Wextra -Wpedantic -Wunused-variable)
  local -a cxxflags=(-std=c++20 -O3 -DNDEBUG "${warnflags[@]}" "${ARCH_FLAGS[@]}" "${LTO_COMPILE_FLAGS[@]}" -pthread)
  local -a ldflags=(-std=c++20 -O3 -DNDEBUG "${ARCH_FLAGS[@]}" "${LTO_LINK_FLAGS[@]}" -pthread)
  local -a mpath=(-fprebuilt-module-path=.)

  # 1) Precompile module interfaces -> .pcm (BMI)
  run_cxx "${cxxflags[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_interface.cxx -o geister_interface.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank_obsblk.cxx -o geister_rank_obsblk.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile tablebase_io.cxx -o tablebase_io.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_tb_handler.cxx -o geister_tb_handler.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_random_player.cxx -o geister_random_player.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_proven_escape.cxx -o geister_proven_escape.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_purple_winning.cxx -o geister_purple_winning.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile confident_player.cxx -o confident_player.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  run_cxx "${cxxflags[@]}" -c geister_core.cxx -o geister_core.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_interface.cxx -o geister_interface.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank.cxx -o geister_rank.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank_triplet.cxx -o geister_rank_triplet.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank_obsblk.cxx -o geister_rank_obsblk.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c tablebase_io.cxx -o tablebase_io.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_tb_handler.cxx -o geister_tb_handler.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_random_player.cxx -o geister_random_player.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_proven_escape.cxx -o geister_proven_escape.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_purple_winning.cxx -o geister_purple_winning.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c confident_player.cxx -o confident_player.o

  # 3) Compile the stdio baseline player (imports modules via BMIs)
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_stdio_baseline_player.cpp -o geister_stdio_baseline_player.o

  # 4) Link
  run_cxx "${ldflags[@]}" \
    geister_stdio_baseline_player.o \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o geister_proven_escape.o geister_purple_winning.o confident_player.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_stdio_baseline_player

  rm -f \
    geister_stdio_baseline_player.o \
    geister_core.pcm geister_interface.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_tb_handler.pcm geister_random_player.pcm geister_proven_escape.pcm geister_purple_winning.pcm confident_player.pcm \
    geister_core.o geister_interface.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o \
    geister_tb_handler.o geister_random_player.o geister_proven_escape.o geister_purple_winning.o confident_player.o
}

build_tests() {
  clean_test_artifacts

  local -a warnflags=(-Wall -Wextra -Wpedantic -Wunused-variable)
  local -a cxxflags=(-std=c++20 -O3 "${warnflags[@]}" "${ARCH_FLAGS[@]}" "${LTO_COMPILE_FLAGS[@]}")
  local -a ldflags=(-std=c++20 -O3 "${ARCH_FLAGS[@]}" "${LTO_LINK_FLAGS[@]}")
  local -a mpath=(-fprebuilt-module-path=.)

  # 1) Precompile module interfaces -> .pcm (BMI)
  run_cxx "${cxxflags[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  run_cxx "${cxxflags[@]}" -c geister_core.cxx -o geister_core.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank.cxx -o geister_rank.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank_triplet.cxx -o geister_rank_triplet.o

  # 3) Compile the blackbox tests (imports modules via BMIs)
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -DGEISTER_ENABLE_TRIPLET_RANK_TESTS -c geister_blackbox_tests.cpp -o geister_blackbox_tests.o

  # 4) Link
  run_cxx "${ldflags[@]}" \
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

  if ! detect_ompflags; then
    echo 'Error: failed to auto-detect OpenMP flags for builders.' >&2
    echo 'Retry with: ./build_public.sh --omp-flags "..." builders' >&2
    exit 1
  fi

  local -a warnflags=(-Wall -Wextra -Wpedantic -Wunused-variable)
  local -a cxxflags=(-std=c++20 -O3 -DNDEBUG "${warnflags[@]}" "${ARCH_FLAGS[@]}" "${LTO_COMPILE_FLAGS[@]}" "${OMP_COMPILE_FLAGS[@]}" -pthread)
  local -a ldflags=(-std=c++20 -O3 -DNDEBUG "${ARCH_FLAGS[@]}" "${LTO_LINK_FLAGS[@]}" "${OMP_LINK_FLAGS[@]}" -pthread)
  local -a mpath=(-fprebuilt-module-path=.)

  # 1) Precompile module interfaces -> .pcm (BMI)
  run_cxx "${cxxflags[@]}" -x c++-module --precompile geister_core.cxx -o geister_core.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank.cxx -o geister_rank.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank_triplet.cxx -o geister_rank_triplet.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile geister_rank_obsblk.cxx -o geister_rank_obsblk.pcm
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -x c++-module --precompile tablebase_io.cxx -o tablebase_io.pcm

  # 2) Compile object files from the ORIGINAL sources (NOT from .pcm)
  run_cxx "${cxxflags[@]}" -c geister_core.cxx -o geister_core.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank.cxx -o geister_rank.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank_triplet.cxx -o geister_rank_triplet.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_rank_obsblk.cxx -o geister_rank_obsblk.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c tablebase_io.cxx -o tablebase_io.o

  # 3) Compile builder translation units (imports modules via BMIs)
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_perfect_information_tb.cpp -o geister_perfect_information_tb.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_purple_tb.cpp -o geister_purple_tb.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_perfect_information_tb_9_10.cpp -o geister_perfect_information_tb_9_10.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_perfect_information_tb_9_10_repack_obsblk.cpp -o geister_perfect_information_tb_9_10_repack_obsblk.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_purple_tb_red2.cpp -o geister_purple_tb_red2.o
  run_cxx "${cxxflags[@]}" "${mpath[@]}" -c geister_purple_tb_red2_repack.cpp -o geister_purple_tb_red2_repack.o

  # 4) Link
  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb

  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o tablebase_io.o geister_purple_tb.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_purple_tb

  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb_9_10.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb_9_10

  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_perfect_information_tb_9_10_repack_obsblk

  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o tablebase_io.o geister_purple_tb_red2.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_purple_tb_red2

  run_cxx "${ldflags[@]}" \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_purple_tb_red2_repack.o \
    "${SEEK_OBJ}" "${ZSTD_STATIC_LIB}" \
    -o geister_purple_tb_red2_repack

  rm -f \
    geister_perfect_information_tb.o geister_purple_tb.o \
    geister_perfect_information_tb_9_10.o geister_perfect_information_tb_9_10_repack_obsblk.o \
    geister_purple_tb_red2.o geister_purple_tb_red2_repack.o \
    geister_core.pcm geister_rank.pcm geister_rank_triplet.pcm geister_rank_obsblk.pcm tablebase_io.pcm \
    geister_core.o geister_rank.o geister_rank_triplet.o geister_rank_obsblk.o tablebase_io.o
}

if [[ "${TARGET}" == 'clean' ]]; then
  clean_all_artifacts
  exit 0
fi

CXX="$(detect_cxx || true)"
if [[ -z "${CXX}" ]]; then
  echo 'Error: clang++ not found (tried clang++ and clang++-NN)' >&2
  exit 1
fi

configure_arch_flags
configure_lto_flags

log "CXX: ${CXX}"
if [[ "${BUILD_MODE}" == 'native' ]]; then
  log 'arch: native + BMI2 (default)'
else
  log 'arch: portable BMI2-preserving'
fi

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
  *)
    usage >&2
    exit 1
    ;;
esac
