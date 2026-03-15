#!/usr/bin/env bash
set -euo pipefail

# Prepare dependencies for seekable-zstd probing.
#
# This script builds:
#   - third_party/zstd/lib/libzstd.a
#   - build/zstdseek_decompress.o   (contrib/seekable_format decoder)
#
# After running this once, build_public.sh can link against these artifacts.
#
# No environment variables are required. The script auto-detects a usable C
# compiler (preferring clang / versioned clang) and allows optional CLI
# overrides.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BUILD_DIR="${ROOT_DIR}/build"
TP_DIR="${ROOT_DIR}/third_party"
ZSTD_DIR="${TP_DIR}/zstd"

ZSTD_REF='v1.5.7'
CC_BIN=''
CLEAN=0

log() { printf '[%s] %s
' "$(date '+%H:%M:%S')" "$*" >&2; }
die() { printf 'Error: %s
' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

usage() {
  cat <<'USAGE'
Usage: ./prepare_seekable_zstd.sh [options]

Options:
  --ref TAG        zstd git tag / ref to checkout exactly (default: v1.5.7)
  --cc PATH        C compiler for zstdseek_decompress.c (auto-detect if omitted)
  --clean          Remove generated seekable-zstd artifacts before rebuilding
  -h, --help       Show this help

Examples:
  ./prepare_seekable_zstd.sh
  ./prepare_seekable_zstd.sh --cc /usr/bin/clang-18
  ./prepare_seekable_zstd.sh --ref v1.5.7
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      shift
      [[ $# -gt 0 ]] || die '--ref requires an argument'
      ZSTD_REF="$1"
      ;;
    --cc)
      shift
      [[ $# -gt 0 ]] || die '--cc requires an argument'
      CC_BIN="$1"
      ;;
    --clean)
      CLEAN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
  shift
done

best_versioned_cmd() {
  local pattern="$1"
  local candidate
  candidate="$({ compgen -c | grep -E "^${pattern}-[0-9]+$" || true; } | sort -Vu | tail -n 1)"
  [[ -n "$candidate" ]] && command -v "$candidate"
}

resolve_clang_c() {
  if [[ -n "$CC_BIN" ]]; then
    printf '%s
' "$CC_BIN"
    return 0
  fi
  if have clang; then command -v clang; return 0; fi
  local c=''
  c="$(best_versioned_cmd 'clang' || true)"
  if [[ -n "$c" ]]; then
    printf '%s
' "$c"
    return 0
  fi
  return 1
}

cpu_jobs() {
  if have nproc; then nproc
  elif [[ "$(uname -s)" == 'Darwin' ]] && have sysctl; then sysctl -n hw.ncpu
  else echo 4
  fi
}

pick_make() {
  if have make && make --version 2>/dev/null | grep -qi 'GNU Make'; then
    echo make
  elif have gmake; then
    echo gmake
  else
    echo make
  fi
}

clean_outputs() {
  rm -f "${BUILD_DIR}/zstdseek_decompress.o"
  rm -f "${ZSTD_DIR}/lib/libzstd.a"
}

ensure_tools() {
  have git || die 'git not found'
  local make_bin
  make_bin="$(pick_make)"
  have "$make_bin" || die 'make not found'
  CC_BIN="$(resolve_clang_c || true)"
  [[ -n "$CC_BIN" ]] || die 'clang compiler not found (tried clang and versioned clang like clang-18)'
  [[ -x "$CC_BIN" ]] || have "$CC_BIN" || die "compiler not found: ${CC_BIN}"
  log "Using CC:   ${CC_BIN}"
  log "Using make: ${make_bin}"
}

require_resolved_ref() {
  local repo_dir="$1"
  local ref="$2"
  git -C "$repo_dir" rev-parse --verify "${ref}^{commit}" >/dev/null 2>&1 ||     die "requested zstd ref is not available locally: ${ref}"
}

checkout_exact_ref() {
  local repo_dir="$1"
  local ref="$2"
  local want have

  require_resolved_ref "$repo_dir" "$ref"
  want="$(git -C "$repo_dir" rev-parse --verify "${ref}^{commit}")"
  git -C "$repo_dir" checkout -q "$want" || die "failed to checkout zstd ref: ${ref}"
  have="$(git -C "$repo_dir" rev-parse --verify HEAD)"
  [[ "$have" == "$want" ]] || die "checked out zstd commit ${have}, expected ${want} for ref ${ref}"
  log "Pinned zstd to ${ref} (${have})"
}

clone_or_update_zstd() {
  mkdir -p "${TP_DIR}"

  if [[ -e "${ZSTD_DIR}" && ! -d "${ZSTD_DIR}" ]]; then
    die "path exists but is not a directory: ${ZSTD_DIR}"
  fi

  if [[ -d "${ZSTD_DIR}/.git" ]]; then
    log "Updating existing zstd repo: ${ZSTD_DIR}"
    (
      cd "${ZSTD_DIR}"
      git fetch --tags --quiet || log "Warning: git fetch --tags failed; continuing with local refs only."
    )
    checkout_exact_ref "${ZSTD_DIR}" "${ZSTD_REF}"
    return
  fi

  if [[ -d "${ZSTD_DIR}" ]]; then
    die "existing ${ZSTD_DIR} is not a git checkout. Remove it and rerun so the script can clone the pinned zstd ref."
  fi

  log "Cloning zstd into ${ZSTD_DIR} ..."
  git clone https://github.com/facebook/zstd.git "${ZSTD_DIR}" || die "failed to clone zstd into ${ZSTD_DIR}"
  checkout_exact_ref "${ZSTD_DIR}" "${ZSTD_REF}"
}

build_zstd_static_lib() {
  local make_bin jobs
  make_bin="$(pick_make)"
  jobs="$(cpu_jobs)"

  log "Building libzstd.a (static) using ${make_bin} (-j${jobs}) ..."
  (
    cd "${ZSTD_DIR}"
    "${make_bin}" -j"${jobs}" CC="${CC_BIN}"
  )

  [[ -f "${ZSTD_DIR}/lib/libzstd.a" ]] || die "libzstd.a not found at ${ZSTD_DIR}/lib/libzstd.a"
}

build_seekable_decompress_obj() {
  mkdir -p "${BUILD_DIR}"

  local src="${ZSTD_DIR}/contrib/seekable_format/zstdseek_decompress.c"
  [[ -f "${src}" ]] || die "missing seekable decoder source: ${src}"

  local cflags_common=(
    -O3 -DNDEBUG
    -I"${ZSTD_DIR}/contrib/seekable_format"
    -I"${ZSTD_DIR}/lib"
    -I"${ZSTD_DIR}/lib/common"
    -DXXH_NAMESPACE=ZSTD_
  )

  log "Compiling seekable_format decoder object: ${BUILD_DIR}/zstdseek_decompress.o"
  "${CC_BIN}" "${cflags_common[@]}"     -c "${src}"     -o "${BUILD_DIR}/zstdseek_decompress.o"

  [[ -f "${BUILD_DIR}/zstdseek_decompress.o" ]] || die "failed to produce ${BUILD_DIR}/zstdseek_decompress.o"
}

main() {
  if [[ $CLEAN -eq 1 ]]; then
    clean_outputs
  fi
  ensure_tools
  clone_or_update_zstd
  build_zstd_static_lib
  build_seekable_decompress_obj

  echo
  echo 'OK: seekable-zstd deps are ready.'
  echo "  C compiler:         ${CC_BIN}"
  echo "  zstd ref:           ${ZSTD_REF}"
  echo "  ZSTD seekable obj:  ${BUILD_DIR}/zstdseek_decompress.o"
  echo "  ZSTD static lib:    ${ZSTD_DIR}/lib/libzstd.a"
  echo
  echo 'Next: run ./build_public.sh all'
}

main "$@"
