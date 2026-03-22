#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

usage() {
  cat <<'USAGE'
Usage: ./run_public_tb_pipeline.sh [options]

Build/run order encoded by this script:
  0) blackbox tests                         (geister_blackbox_tests)
  1) <=8 perfect-information TBs            (geister_perfect_information_tb)
  2) 9/10 perfect-information partitioned   (geister_perfect_information_tb_9_10)
  3) 9/10 perfect obsblk repack             (geister_perfect_information_tb_9_10_repack_obsblk)
  4) <=8 purple TBs                         (geister_purple_tb)
  5) red2 purple p9a partitions             (geister_purple_tb_red2 --only p9a)
  6) red2 purple p9b partitions             (geister_purple_tb_red2 --only p9b)
  7) red2 purple p10 partitions             (geister_purple_tb_red2 --only p10)
  8) red2 purple N-side repack              (geister_purple_tb_red2_repack --turn N)
  9) compress generated *.bin -> *.bin.zst  (optional)

Important:
  - This script runs geister_blackbox_tests first and aborts immediately on failure.
  - This script always builds <=8 perfect tables and purple totals 3..8, because
    the 9/10 perfect builder depends on <=8 perfect tables and the red2 purple
    builder depends on the legacy <=8 purple tables.
  - geister_perfect_information_tb is run with --no-write-txt, so normal output is *_obsblk.bin only.
  - geister_perfect_information_tb_9_10 consumes <=8 *_obsblk.bin directly
    (legacy headerless .bin is also accepted as a fallback).
  - geister_purple_tb has no --out-dir flag; this pipeline runs it inside OUT_DIR so the
    legacy tb_purple_{N,P}_*.txt and tb_purple_{N,P}_*.bin files land there.
  - Runtime-relevant red2 purple outputs are repacked as single-file tb_purple_N_*.bin only;
    tb_purple_P_* and partition files remain builder-internal / intermediate forms.

Options:
  --repo-dir DIR             Repo/build directory containing the built binaries (default: current dir)
  --out-dir DIR              Output directory for generated tables (default: ./tb_out)
  --perfect-dep-dir DIR      Directory containing <=8 perfect *_obsblk.bin deps for 9/10 perfect build
                             (legacy headerless .bin also accepted; default: same as --out-dir)
  --purple-red2-stage DIR    Staging directory used by geister_purple_tb_red2_repack
                             (default: OUT_DIR/.tb_purple_red2_stage)
  --max-depth N              DTW horizon / final iteration number (default: 210)
  --compress                 Attempt final compression (default; auto-skips if helper is unavailable)
  --no-compress              Skip final compression
  --require-compress         Fail instead of skipping when no usable compressor helper is available
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
  printf '[%s] %s
' "$(date '+%H:%M:%S')" "$*" >&2
}

die() {
  printf 'Error: %s
' "$*" >&2
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
    (cd "$dir" && printf '%s/%s
' "$(pwd -P)" "$base")
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
    printf '%s
' "$candidate"
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
PURPLE_RED2_STAGE_DIR=""
MAX_DEPTH=210
DO_COMPRESS=1
COMPRESS_REQUIRED=0
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
    --purple-red2-stage)
      shift; [[ $# -gt 0 ]] || die "missing value for --purple-red2-stage"
      PURPLE_RED2_STAGE_DIR="$(abspath "$1")"
      ;;
    --compress)
      DO_COMPRESS=1
      ;;
    --no-compress)
      DO_COMPRESS=0
      ;;
    --require-compress)
      COMPRESS_REQUIRED=1
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

if [[ -z "$PURPLE_RED2_STAGE_DIR" ]]; then
  PURPLE_RED2_STAGE_DIR="$OUT_DIR/.tb_purple_red2_stage"
fi

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
PURPLE_LE8_BIN="$REPO_DIR/geister_purple_tb"
PURPLE_RED2_BIN="$REPO_DIR/geister_purple_tb_red2"
PURPLE_RED2_REPACK_BIN="$REPO_DIR/geister_purple_tb_red2_repack"

need_exec "$BLACKBOX_TESTS_BIN"
need_exec "$PERFECT_LE8_BIN"
need_exec "$PERFECT_9_10_BIN"
need_exec "$PERFECT_9_10_REPACK_BIN"
need_exec "$PURPLE_LE8_BIN"
need_exec "$PURPLE_RED2_BIN"
need_exec "$PURPLE_RED2_REPACK_BIN"

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

detect_cxx() {
  if [[ -n "$CXX_OVERRIDE" ]]; then
    if resolved="$(resolve_exec_path "$CXX_OVERRIDE" 2>/dev/null)"; then
      printf '%s
' "$resolved"
      return 0
    fi
    die "compiler not found: $CXX_OVERRIDE"
  fi

  local cxx resolved
  for cxx in     /usr/lib/llvm-20/bin/clang++     /usr/bin/clang++-20     clang++-20     clang++     clang++-19     clang++-18     clang++-17     clang++-16     g++     g++-14     g++-13     g++-12     g++-11; do
    if resolved="$(resolve_exec_path "$cxx" 2>/dev/null)"; then
      printf '%s
' "$resolved"
      return 0
    fi
  done
  return 1
}

find_existing_compressor_binary() {
  local candidate
  if [[ -n "$COMPRESSOR_BIN" ]]; then
    [[ -x "$COMPRESSOR_BIN" ]] || die "compressor binary not executable: $COMPRESSOR_BIN"
    printf '%s
' "$COMPRESSOR_BIN"
    return 0
  fi

  for candidate in     "$REPO_DIR/seekable_zstd_multithread"     "$SCRIPT_DIR/seekable_zstd_multithread"; do
    if [[ -x "$candidate" ]]; then
      printf '%s
' "$candidate"
      return 0
    fi
  done

  return 1
}

find_compressor_source() {
  local candidate
  if [[ -n "$COMPRESSOR_SRC" ]]; then
    [[ -f "$COMPRESSOR_SRC" ]] || die "compressor source not found: $COMPRESSOR_SRC"
    printf '%s
' "$COMPRESSOR_SRC"
    return 0
  fi

  for candidate in     "$REPO_DIR/seekable_zstd_multithread.cpp"     "$SCRIPT_DIR/seekable_zstd_multithread.cpp"; do
    if [[ -f "$candidate" ]]; then
      printf '%s
' "$candidate"
      return 0
    fi
  done

  return 1
}

build_compressor_from_source() {
  local src="$1"
  local cxx="$2"
  local outbin="$REPO_DIR/seekable_zstd_multithread"

  log "building compressor: $outbin"
  "$cxx" -std=c++20 -O3 -DNDEBUG -pthread "$src" -lzstd -o "$outbin" || return 1
  [[ -x "$outbin" ]] || return 1
  printf '%s
' "$outbin"
  return 0
}

ensure_compressor_strict() {
  local existing src cxx built

  if existing="$(find_existing_compressor_binary 2>/dev/null)"; then
    printf '%s
' "$existing"
    return 0
  fi

  src="$(find_compressor_source 2>/dev/null || true)"
  [[ -n "${src:-}" ]] || die "compression required, but no compressor binary/source found. Provide --compressor or --compressor-src."

  cxx="$(detect_cxx || true)"
  [[ -n "$cxx" ]] || die "failed to detect a C++ compiler for building the compressor"

  built="$(build_compressor_from_source "$src" "$cxx" || true)"
  [[ -n "$built" && -x "$built" ]] || die "failed to build compressor from source: $src"
  printf '%s
' "$built"
}

maybe_prepare_compressor() {
  local existing src cxx built

  if [[ -n "$COMPRESSOR_BIN" || -n "$COMPRESSOR_SRC" || "$COMPRESS_REQUIRED" -eq 1 ]]; then
    ensure_compressor_strict
    return 0
  fi

  if existing="$(find_existing_compressor_binary 2>/dev/null)"; then
    printf '%s
' "$existing"
    return 0
  fi

  src="$(find_compressor_source 2>/dev/null || true)"
  if [[ -z "${src:-}" ]]; then
    return 1
  fi

  cxx="$(detect_cxx || true)"
  if [[ -z "$cxx" ]]; then
    log "compression helper source found, but no C++ compiler is available; skipping final compression"
    return 1
  fi

  built="$(build_compressor_from_source "$src" "$cxx" || true)"
  if [[ -n "$built" && -x "$built" ]]; then
    printf '%s
' "$built"
    return 0
  fi

  log "failed to build compression helper from $src; skipping final compression"
  return 1
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

has_raw_bins() {
  find "$OUT_DIR" -type f -name '*.bin' ! -name '*.bin.zst' ! -name '*.bin.zstd' -print -quit | grep -q .
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
log "purple_red2_stage_dir=$PURPLE_RED2_STAGE_DIR"
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

# 4) <=8 purple TBs (all meaningful totals 3..8).
# geister_purple_tb has no --out-dir; it writes to cwd.
run_cmd_in_dir "$OUT_DIR" "$PURPLE_LE8_BIN" --total-min 3 --total-max 8

# 5) red2 purple TB partitions.
# p9a / p9b depend on <=8 purple legacy tb_purple_{N,P}_*.bin in OUT_DIR.
run_cmd "$PURPLE_RED2_BIN" --out-dir "$OUT_DIR" --dep-dir "$OUT_DIR" --max-depth "$MAX_DEPTH" --only p9a
run_cmd "$PURPLE_RED2_BIN" --out-dir "$OUT_DIR" --dep-dir "$OUT_DIR" --max-depth "$MAX_DEPTH" --only p9b
# p10 additionally depends on the completed p9b iteration from the same out-dir.
run_cmd "$PURPLE_RED2_BIN" --out-dir "$OUT_DIR" --dep-dir "$OUT_DIR" --max-depth "$MAX_DEPTH" --only p10 --dep-p9b-iter "$MAX_DEPTH"

# 8) Repack red2 purple partitioned output into runtime single-file tb_purple_N_*.bin.
run_cmd "$PURPLE_RED2_REPACK_BIN" --in "$OUT_DIR" --out "$OUT_DIR" --iter "$MAX_DEPTH" --turn N --stage "$PURPLE_RED2_STAGE_DIR"

# 9) Optional final compression of generated raw *.bin files.
if (( DO_COMPRESS )); then
  if has_raw_bins; then
    if COMPRESSOR_BIN_REAL="$(maybe_prepare_compressor)"; then
      log "compressor=$COMPRESSOR_BIN_REAL"
      compress_bins_tree "$COMPRESSOR_BIN_REAL"
    else
      log "compression helper unavailable; skipping final compression and keeping raw *.bin files"
    fi
  else
    log "no raw *.bin files found; compression skipped"
  fi
else
  log "compression skipped (--no-compress)"
fi

log "done"
