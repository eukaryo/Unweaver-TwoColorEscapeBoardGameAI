#!/usr/bin/env bash
set -euo pipefail

# Install/check the public geister repo build dependencies.
#
# This script verifies:
#   - clang / clang++ usable for C++20 named modules
#   - git / make
#   - OpenMP headers+linker support (<omp.h>)
#   - zstd headers+library (<zstd.h>, -lzstd)
#   - optional BMI2 CPU support warning (repo builds with -mbmi2)
#
# No environment variables are required. The build scripts auto-detect versioned
# clang/clang++ as well.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK_ONLY=0
FORCE_PM=''
VERBOSE=0

RESOLVED_CC=''
RESOLVED_CXX=''
RESOLVED_OMP=''

log()  { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
warn() { printf 'Warning: %s\n' "$*" >&2; }
die()  { printf 'Error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

usage() {
  cat <<'USAGE'
Usage: ./install_public_build_deps.sh [options]

Options:
  --check-only      Only verify dependencies; do not install anything
  --pm NAME         Force package manager: apt | dnf | pacman | zypper | brew
  -v, --verbose     Print extra diagnostics
  -h, --help        Show this help

Notes:
  - This script may use sudo for system package installation.
  - No environment setup step is required after this script.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only)
      CHECK_ONLY=1
      ;;
    --pm)
      shift
      [[ $# -gt 0 ]] || die '--pm requires an argument'
      FORCE_PM="$1"
      ;;
    -v|--verbose)
      VERBOSE=1
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

run_root() {
  if [[ $EUID -eq 0 ]]; then
    "$@"
  elif have sudo; then
    sudo "$@"
  else
    die 'need root privileges to install packages; rerun as root or install sudo'
  fi
}

detect_pm() {
  if [[ -n "$FORCE_PM" ]]; then
    echo "$FORCE_PM"
    return
  fi
  if have apt-get; then echo apt; return; fi
  if have dnf; then echo dnf; return; fi
  if have pacman; then echo pacman; return; fi
  if have zypper; then echo zypper; return; fi
  if have brew; then echo brew; return; fi
  die 'unsupported system: no known package manager found'
}

best_versioned_cmd() {
  local pattern="$1"
  local candidate
  candidate="$({ compgen -c | grep -E "^${pattern}-[0-9]+$" || true; } | sort -Vu | tail -n 1)"
  [[ -n "$candidate" ]] && command -v "$candidate"
}

resolve_clang_c() {
  if have clang; then command -v clang; return 0; fi
  local c=''
  c="$(best_versioned_cmd 'clang' || true)"
  if [[ -n "$c" ]]; then printf '%s\n' "$c"; return 0; fi
  if have brew; then
    local brew_llvm=''
    brew_llvm="$(brew --prefix llvm 2>/dev/null || true)"
    if [[ -n "$brew_llvm" && -x "$brew_llvm/bin/clang" ]]; then
      printf '%s\n' "$brew_llvm/bin/clang"
      return 0
    fi
  fi
  return 1
}

resolve_clang_cxx() {
  if have clang++; then command -v clang++; return 0; fi
  local cxx=''
  cxx="$(best_versioned_cmd 'clang\+\+' || true)"
  if [[ -n "$cxx" ]]; then printf '%s\n' "$cxx"; return 0; fi
  if have brew; then
    local brew_llvm=''
    brew_llvm="$(brew --prefix llvm 2>/dev/null || true)"
    if [[ -n "$brew_llvm" && -x "$brew_llvm/bin/clang++" ]]; then
      printf '%s\n' "$brew_llvm/bin/clang++"
      return 0
    fi
  fi
  return 1
}

extract_compiler_version_suffix() {
  local base="$1"
  if [[ "$base" =~ ([0-9]+)$ ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
  fi
}

module_smoke_test() {
  local cxx_bin="$1"
  local tmpdir="$2"
  cat > "${tmpdir}/m.cppm" <<'CPP'
export module m;
export int meaning() { return 42; }
CPP
  "${cxx_bin}" -std=c++20 -x c++-module --precompile "${tmpdir}/m.cppm" -o "${tmpdir}/m.pcm" >/dev/null 2>&1
}

openmp_smoke_test() {
  local cxx_bin="$1"
  local tmpdir="$2"
  shift 2
  cat > "${tmpdir}/omp.cpp" <<'CPP'
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
  "${cxx_bin}" -std=c++20 "$@" "${tmpdir}/omp.cpp" -o "${tmpdir}/omp_test" >/dev/null 2>&1
}

auto_detect_omp_flags() {
  local cxx_bin="$1"
  local tmpdir="$2"
  local -a candidates=()
  candidates+=("-fopenmp")
  candidates+=("-fopenmp=libomp")

  local base ver bindir prefix
  base="$(basename "$cxx_bin")"
  ver="$(extract_compiler_version_suffix "$base" || true)"
  bindir="$(cd "$(dirname "$cxx_bin")" && pwd)"
  prefix="$(cd "${bindir}/.." && pwd)"

  local -a libdirs=()
  [[ -d "${prefix}/lib" ]] && libdirs+=("${prefix}/lib")
  [[ -n "$ver" && -d "/usr/lib/llvm-${ver}/lib" ]] && libdirs+=("/usr/lib/llvm-${ver}/lib")
  [[ -n "$ver" && -d "/usr/lib64/llvm/${ver}/lib" ]] && libdirs+=("/usr/lib64/llvm/${ver}/lib")

  local libdir
  for libdir in "${libdirs[@]}"; do
    candidates+=("-fopenmp -L${libdir} -Wl,-rpath,${libdir}")
    candidates+=("-fopenmp=libomp -L${libdir} -Wl,-rpath,${libdir}")
  done

  local cand
  for cand in "${candidates[@]}"; do
    local -a arr=()
    read -r -a arr <<< "$cand"
    if openmp_smoke_test "$cxx_bin" "$tmpdir" "${arr[@]}"; then
      printf '%s\n' "$cand"
      return 0
    fi
  done
  return 1
}

zstd_smoke_test() {
  local cxx_bin="$1"
  local tmpdir="$2"
  cat > "${tmpdir}/zstd.cpp" <<'CPP'
#include <zstd.h>
int main() {
  return ZSTD_versionNumber() <= 0;
}
CPP
  "${cxx_bin}" -std=c++20 "${tmpdir}/zstd.cpp" -lzstd -o "${tmpdir}/zstd_test" >/dev/null 2>&1
}

check_bmi2() {
  if [[ -r /proc/cpuinfo ]]; then
    if ! grep -qw bmi2 /proc/cpuinfo; then
      warn 'CPU flag bmi2 not found in /proc/cpuinfo; this repo builds with -mbmi2'
    fi
  elif have sysctl; then
    local features
    features="$(sysctl -n machdep.cpu.leaf7_features 2>/dev/null || true) $(sysctl -n machdep.cpu.features 2>/dev/null || true)"
    if ! grep -qi 'BMI2' <<<"$features"; then
      warn 'Could not confirm BMI2 support; this repo builds with -mbmi2'
    fi
  fi
}

check_all() {
  local failures=0

  have git  || { warn 'missing command: git'; failures=$((failures + 1)); }
  have make || { warn 'missing command: make'; failures=$((failures + 1)); }

  RESOLVED_CC="$(resolve_clang_c || true)"
  RESOLVED_CXX="$(resolve_clang_cxx || true)"

  if [[ -z "$RESOLVED_CC" ]]; then
    warn 'clang compiler not found'
    failures=$((failures + 1))
  fi
  if [[ -z "$RESOLVED_CXX" ]]; then
    warn 'clang++ compiler not found'
    failures=$((failures + 1))
  fi

  local tmpdir
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' RETURN

  if [[ -n "$RESOLVED_CXX" ]]; then
    module_smoke_test "$RESOLVED_CXX" "$tmpdir" || {
      warn "C++20 named module smoke test failed with ${RESOLVED_CXX}"
      failures=$((failures + 1))
    }

    RESOLVED_OMP="$(auto_detect_omp_flags "$RESOLVED_CXX" "$tmpdir" || true)"
    if [[ -z "$RESOLVED_OMP" ]]; then
      warn "OpenMP smoke test failed with ${RESOLVED_CXX}"
      failures=$((failures + 1))
    fi

    zstd_smoke_test "$RESOLVED_CXX" "$tmpdir" || {
      warn "zstd smoke test failed with ${RESOLVED_CXX} (-lzstd / <zstd.h>)"
      failures=$((failures + 1))
    }
  fi

  check_bmi2

  if [[ $VERBOSE -eq 1 ]]; then
    [[ -n "$RESOLVED_CC" ]] && log "Resolved clang:   $RESOLVED_CC"
    [[ -n "$RESOLVED_CXX" ]] && log "Resolved clang++: $RESOLVED_CXX"
    [[ -n "$RESOLVED_OMP" ]] && log "Resolved OpenMP:  $RESOLVED_OMP"
    have git  && log "git:  $(command -v git)"
    have make && log "make: $(command -v make)"
  fi

  if [[ $failures -eq 0 ]]; then
    log 'Dependency check passed.'
    return 0
  fi
  return 1
}

install_with_apt() {
  run_root env DEBIAN_FRONTEND=noninteractive apt-get update
  run_root env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential git make clang lld libomp-dev libzstd-dev zstd pkg-config ca-certificates
}

install_with_dnf() {
  run_root dnf install -y \
    gcc-c++ make git clang lld libomp libomp-devel zstd zstd-devel pkgconf-pkg-config ca-certificates
}

install_with_pacman() {
  run_root pacman -Sy --needed --noconfirm \
    base-devel git clang lld llvm-openmp zstd pkgconf ca-certificates
}

install_with_zypper() {
  run_root zypper --non-interactive refresh
  run_root zypper --non-interactive install -y \
    gcc-c++ make git clang lld libomp-devel libzstd-devel zstd pkg-config ca-certificates
}

install_with_brew() {
  have brew || die 'brew not found'
  brew update
  brew install llvm zstd git make pkg-config || true
}

install_deps() {
  local pm
  pm="$(detect_pm)"
  log "Using package manager: ${pm}"
  case "$pm" in
    apt)    install_with_apt ;;
    dnf)    install_with_dnf ;;
    pacman) install_with_pacman ;;
    zypper) install_with_zypper ;;
    brew)   install_with_brew ;;
    *) die "unsupported package manager: ${pm}" ;;
  esac
}

print_next_steps() {
  echo
  echo 'Next:'
  echo '  ./prepare_seekable_zstd.sh'
  echo '  ./build_public.sh all'
}

main() {
  if check_all; then
    echo
    echo 'All required dependencies already look usable.'
    [[ -n "$RESOLVED_CC" ]] && echo "Detected clang:   ${RESOLVED_CC}"
    [[ -n "$RESOLVED_CXX" ]] && echo "Detected clang++: ${RESOLVED_CXX}"
    [[ -n "$RESOLVED_OMP" ]] && echo "Detected OpenMP flags: ${RESOLVED_OMP}"
    print_next_steps
    exit 0
  fi

  if [[ $CHECK_ONLY -eq 1 ]]; then
    die 'dependency check failed'
  fi

  echo
  log 'Installing missing dependencies ...'
  install_deps

  echo
  log 'Re-checking after installation ...'
  check_all || die 'dependencies still incomplete after installation'

  echo
  echo 'OK: build dependencies look ready.'
  [[ -n "$RESOLVED_CC" ]] && echo "Detected clang:   ${RESOLVED_CC}"
  [[ -n "$RESOLVED_CXX" ]] && echo "Detected clang++: ${RESOLVED_CXX}"
  [[ -n "$RESOLVED_OMP" ]] && echo "Detected OpenMP flags: ${RESOLVED_OMP}"
  print_next_steps
}

main "$@"
