#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./run_public_tb_pipeline.sh [options]

Build/run order encoded by this script:
  0) blackbox tests                       (geister_blackbox_tests)
  1) <=8 perfect-information TBs          (geister_perfect_information_tb)
  2) 9/10 perfect-information partitioned (geister_perfect_information_tb_9_10)
  3) 9/10 perfect obsblk repack           (geister_perfect_information_tb_9_10_repack_obsblk)
  4) compress generated *.bin -> *.bin.zst  (optional)

Important:
  - This script runs geister_blackbox_tests first and aborts immediately on failure.
  - This script always builds <=8 up to total=8, because the 9/10 perfect builder depends on <=8 tables.
  - geister_perfect_information_tb is run with --no-write-txt, so normal output is *_obsblk.bin only.
  - geister_perfect_information_tb_9_10 consumes <=8 *_obsblk.bin directly
    (legacy headerless .bin is also accepted as a fallback).

Options:
  --repo-dir DIR             Repo/build directory containing the built binaries (default: current dir)
  --out-dir DIR              Output directory for generated tables (default: ./tb_out)
  --perfect-dep-dir DIR      Directory containing <=8 perfect *_obsblk.bin deps for 9/10 perfect build
                             (legacy headerless .bin also accepted; default: same as --out-dir)
  --max-depth N              DTW horizon / final iteration number (default: 210)
  --compress                 Compress generated *.bin files at the end (default)
  --no-compress              Skip final compression
  --delete-bin-after-compress
                             Remove raw *.bin after successful compression (default: keep raw *.bin)
  --compressor PATH          Existing seekable-zstd compressor binary to use
  --compressor-src PATH      Compressor source file to build if needed
  --cxx PATH                 C++ compiler for building the compressor if needed
  --compress-j N             Compressor worker threads (default: nproc / hw.ncpu / 1)
  --compress-batch-frames N  Compressor -b argument (default: 256)
  --compress-inflight N      Compressor -i argument (default: 2 * threads)
  -h, --help                 Show this help
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

have() {
  command -v "$1" >/dev/null 2>&1
}

abspath() {
  local p="$1"
  if [[ -d "$p" ]]; then
    (cd "$p" && pwd -P)
  else
    local dir base
    dir="$(dirname "$p")"
    base="$(basename "$p")"
    (cd "$dir" && printf '%s/%s\n' "$(pwd -P)" "$base")
  fi
}

cpu_jobs() {
  if have nproc; then
    nproc
  elif [[ "$(uname -s)" == "Darwin" ]] && have sysctl; then
    sysctl -n hw.ncpu
  else
    echo 1
  fi
}

resolve_exec_path() {
  local candidate="$1"
  if [[ -x "$candidate" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi
  if have "$candidate"; then
    command -v "$candidate"
    return 0
  fi
  return 1
}

REPO_DIR="$(pwd -P)"
OUT_DIR="$(pwd -P)/tb_out"
PERFECT_DEP_DIR=""
MAX_DEPTH=210
DO_COMPRESS=1
DELETE_RAW_BIN=0
COMPRESSOR_BIN=""
COMPRESSOR_SRC=""
CXX_OVERRIDE=""
COMPRESS_JOBS=""
COMPRESS_BATCH_FRAMES=256
COMPRESS_INFLIGHT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir)
      shift; [[ $# -gt 0 ]] || die "missing value for --repo-dir"
      REPO_DIR="$(abspath "$1")"
      ;;
    --out-dir)
      shift; [[ $# -gt 0 ]] || die "missing value for --out-dir"
      mkdir -p "$1"
      OUT_DIR="$(abspath "$1")"
      ;;
    --perfect-dep-dir)
      shift; [[ $# -gt 0 ]] || die "missing value for --perfect-dep-dir"
      PERFECT_DEP_DIR="$(abspath "$1")"
      ;;
    --max-depth)
      shift; [[ $# -gt 0 ]] || die "missing value for --max-depth"
      MAX_DEPTH="$1"
      ;;
    --compress)
      DO_COMPRESS=1
      ;;
    --no-compress)
      DO_COMPRESS=0
      ;;
    --delete-bin-after-compress)
      DELETE_RAW_BIN=1
      ;;
    --compressor)
      shift; [[ $# -gt 0 ]] || die "missing value for --compressor"
      COMPRESSOR_BIN="$(abspath "$1")"
      ;;
    --compressor-src)
      shift; [[ $# -gt 0 ]] || die "missing value for --compressor-src"
      COMPRESSOR_SRC="$(abspath "$1")"
      ;;
    --cxx)
      shift; [[ $# -gt 0 ]] || die "missing value for --cxx"
      CXX_OVERRIDE="$1"
      ;;
    --compress-j)
      shift; [[ $# -gt 0 ]] || die "missing value for --compress-j"
      COMPRESS_JOBS="$1"
      ;;
    --compress-batch-frames)
      shift; [[ $# -gt 0 ]] || die "missing value for --compress-batch-frames"
      COMPRESS_BATCH_FRAMES="$1"
      ;;
    --compress-inflight)
      shift; [[ $# -gt 0 ]] || die "missing value for --compress-inflight"
      COMPRESS_INFLIGHT="$1"
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

[[ -d "$REPO_DIR" ]] || die "repo dir not found: $REPO_DIR"
mkdir -p "$OUT_DIR"
OUT_DIR="$(abspath "$OUT_DIR")"

if [[ -z "$PERFECT_DEP_DIR" ]]; then
  PERFECT_DEP_DIR="$OUT_DIR"
fi
[[ -d "$PERFECT_DEP_DIR" ]] || die "perfect dep dir not found: $PERFECT_DEP_DIR"

case "$MAX_DEPTH" in
  ''|*[!0-9]*) die "--max-depth must be a positive integer" ;;
esac
if (( MAX_DEPTH < 1 || MAX_DEPTH > 210 )); then
  die "--max-depth must be in [1, 210]"
fi

case "$COMPRESS_BATCH_FRAMES" in
  ''|*[!0-9]*) die "--compress-batch-frames must be a positive integer" ;;
esac
if (( COMPRESS_BATCH_FRAMES < 1 )); then
  die "--compress-batch-frames must be >= 1"
fi

if [[ -z "$COMPRESS_JOBS" ]]; then
  COMPRESS_JOBS="$(cpu_jobs)"
fi
case "$COMPRESS_JOBS" in
  ''|*[!0-9]*) die "--compress-j must be a positive integer" ;;
esac
if (( COMPRESS_JOBS < 1 )); then
  die "--compress-j must be >= 1"
fi

if [[ -z "$COMPRESS_INFLIGHT" ]]; then
  COMPRESS_INFLIGHT="$(( COMPRESS_JOBS * 2 ))"
fi
case "$COMPRESS_INFLIGHT" in
  ''|*[!0-9]*) die "--compress-inflight must be a positive integer" ;;
esac
if (( COMPRESS_INFLIGHT < 1 )); then
  die "--compress-inflight must be >= 1"
fi

need_exec() {
  local p="$1"
  [[ -x "$p" ]] || die "required executable not found: $p (run ./build_public.sh all first)"
}

BLACKBOX_TESTS_BIN="$REPO_DIR/geister_blackbox_tests"
PERFECT_LE8_BIN="$REPO_DIR/geister_perfect_information_tb"
PERFECT_9_10_BIN="$REPO_DIR/geister_perfect_information_tb_9_10"
PERFECT_9_10_REPACK_BIN="$REPO_DIR/geister_perfect_information_tb_9_10_repack_obsblk"

need_exec "$BLACKBOX_TESTS_BIN"
need_exec "$PERFECT_LE8_BIN"
need_exec "$PERFECT_9_10_BIN"
need_exec "$PERFECT_9_10_REPACK_BIN"

has_glob_match() {
  local pattern="$1"
  compgen -G "$pattern" >/dev/null 2>&1
}

check_perfect_deps() {
  local a_obs a_legacy b_obs b_legacy
  a_obs="$PERFECT_DEP_DIR"/*_pb2pr1ob4or1_obsblk.bin
  a_legacy="$PERFECT_DEP_DIR"/*_pb2pr1ob4or1.bin
  b_obs="$PERFECT_DEP_DIR"/*_pb3pr1ob3or1_obsblk.bin
  b_legacy="$PERFECT_DEP_DIR"/*_pb3pr1ob3or1.bin

  if ! (has_glob_match "$a_obs" || has_glob_match "$a_legacy"); then
    die "missing <=8 perfect dependency for material *pb2pr1ob4or1 in $PERFECT_DEP_DIR (expected *_obsblk.bin or legacy .bin)."
  fi
  if ! (has_glob_match "$b_obs" || has_glob_match "$b_legacy"); then
    die "missing <=8 perfect dependency for material *pb3pr1ob3or1 in $PERFECT_DEP_DIR (expected *_obsblk.bin or legacy .bin)."
  fi
}

# Build or locate the compressor only if compression is requested.
detect_cxx() {
  if [[ -n "$CXX_OVERRIDE" ]]; then
    if resolved="$(resolve_exec_path "$CXX_OVERRIDE" 2>/dev/null)"; then
      printf '%s\n' "$resolved"
      return 0
    fi
    die "compiler not found: $CXX_OVERRIDE"
  fi

  local cxx resolved
  for cxx in \
    /usr/lib/llvm-20/bin/clang++ \
    /usr/bin/clang++-20 \
    clang++-20 \
    clang++ \
    clang++-19 \
    clang++-18 \
    clang++-17 \
    clang++-16 \
    g++ \
    g++-14 \
    g++-13 \
    g++-12 \
    g++-11; do
    if resolved="$(resolve_exec_path "$cxx" 2>/dev/null)"; then
      printf '%s\n' "$resolved"
      return 0
    fi
  done
  return 1
}

ensure_compressor() {
  local candidate src cxx outbin

  if [[ -n "$COMPRESSOR_BIN" ]]; then
    [[ -x "$COMPRESSOR_BIN" ]] || die "compressor binary not executable: $COMPRESSOR_BIN"
    printf '%s\n' "$COMPRESSOR_BIN"
    return 0
  fi

  for candidate in \
    "$REPO_DIR/seekable_zstd_multithread" \
    "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/seekable_zstd_multithread"; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  if [[ -n "$COMPRESSOR_SRC" ]]; then
    src="$COMPRESSOR_SRC"
  else
    for candidate in \
      "$REPO_DIR/seekable_zstd_multithread.cpp" \
      "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/seekable_zstd_multithread.cpp"; do
      if [[ -f "$candidate" ]]; then
        src="$candidate"
        break
      fi
    done
  fi

  [[ -n "${src:-}" ]] || die "compression requested, but no compressor binary/source found. Provide --compressor or --compressor-src."

  cxx="$(detect_cxx || true)"
  [[ -n "$cxx" ]] || die "failed to detect a C++ compiler for building the compressor"

  outbin="$REPO_DIR/seekable_zstd_multithread"
  log "building compressor: $outbin"
  "$cxx" -std=c++20 -O3 -DNDEBUG -pthread "$src" -lzstd -o "$outbin"
  [[ -x "$outbin" ]] || die "failed to build compressor: $outbin"
  printf '%s\n' "$outbin"
}

run_cmd() {
  log "$*"
  "$@"
}

run_cmd_in_dir() {
  local dir="$1"
  shift
  log "(cd $dir && $*)"
  (
    cd "$dir"
    "$@"
  )
}

compress_bins_tree() {
  local compressor="$1"
  local count=0
  local skipped=0

  while IFS= read -r -d '' f; do
    local out
    out="${f}.zst"

    if [[ -f "$out" && "$out" -nt "$f" ]]; then
      log "skip compression (up-to-date): $f"
      skipped=$(( skipped + 1 ))
      continue
    fi

    run_cmd "$compressor" -j "$COMPRESS_JOBS" -b "$COMPRESS_BATCH_FRAMES" -i "$COMPRESS_INFLIGHT" "$f" "$out"
    if (( DELETE_RAW_BIN )); then
      rm -f "$f"
    fi
    count=$(( count + 1 ))
  done < <(find "$OUT_DIR" -type f -name '*.bin' ! -name '*.bin.zst' ! -name '*.bin.zstd' -print0 | sort -z)

  log "compression summary: compressed=$count skipped=$skipped"
}

log "repo_dir=$REPO_DIR"
log "out_dir=$OUT_DIR"
log "perfect_dep_dir=$PERFECT_DEP_DIR"
log "max_depth=$MAX_DEPTH"

# 0) Run blackbox tests first.
run_cmd_in_dir "$REPO_DIR" "$BLACKBOX_TESTS_BIN"

# 1) <=8 perfect-information TBs.
# geister_perfect_information_tb has no --out-dir; it writes to cwd.
run_cmd_in_dir "$OUT_DIR" "$PERFECT_LE8_BIN" --upto-total 8 --no-write-txt

# 2) 9/10 perfect-information partitioned TBs.
# This consumes <=8 *_obsblk.bin directly (legacy .bin also accepted as fallback).
check_perfect_deps
run_cmd "$PERFECT_9_10_BIN" --out "$OUT_DIR" --dep "$PERFECT_DEP_DIR" --max-depth "$MAX_DEPTH"

# 3) Repack 9/10 perfect partitioned output into runtime *_obsblk.bin files.
run_cmd "$PERFECT_9_10_REPACK_BIN" --in "$OUT_DIR" --out "$OUT_DIR" --iter "$MAX_DEPTH"

# 4) Optional final compression of generated raw *.bin files.
if (( DO_COMPRESS )); then
  COMPRESSOR_BIN_REAL="$(ensure_compressor)"
  log "compressor=$COMPRESSOR_BIN_REAL"
  compress_bins_tree "$COMPRESSOR_BIN_REAL"
else
  log "compression skipped (--no-compress)"
fi

log "done"
