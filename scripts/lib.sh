# Shared helpers for repository scripts. Source it; do not execute it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
# shellcheck source=pins.env
source "$ROOT/scripts/pins.env"

# Ordinary rustup-managed Cargo: rust-toolchain.toml selects the toolchain.
# Prefer rustup's proxies so host wrappers named `cargo` do not intercept.
CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
if [[ -x "$CARGO_BIN/cargo" ]]; then
  export PATH="$CARGO_BIN:$PATH"
fi
# uv must use its own managed, pinned Python (connectors/.python-version).
export UV_PYTHON_PREFERENCE=only-managed

PROTOC_DIR="$ROOT/target/tools/protoc"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# Keep output paths inside this checkout while sharing compiler actions across
# worktrees. Never let an unrelated build collect a running test's executables.
export MBX_TARGET_VIEWS=0
export MBX_GC_MAX_SIZE="${MBX_GC_MAX_SIZE:-10GiB}"
export MBX_GC_MIN_FREE_SIZE="${MBX_GC_MIN_FREE_SIZE:-12GiB}"
export MBX_SUMMARY="${MBX_SUMMARY:-short}"
cargo() {
  case "${1:-}" in
    build|test|check|clippy) mbx "$@" ;;
    *) "$CARGO_BIN/cargo" "$@" ;;
  esac
}

# Fetch the pinned protoc into target/tools once; verify its checksum.
ensure_protoc() {
  local bin="$PROTOC_DIR/bin/protoc"
  if [[ -x "$bin" ]] && "$bin" --version | grep -qx "libprotoc $PROTOC_VERSION"; then
    return 0
  fi
  log "fetching protoc $PROTOC_VERSION"
  local tmp
  tmp="$(mktemp -d)"
  curl -fsSL --retry 3 -o "$tmp/protoc.zip" "$PROTOC_URL"
  echo "$PROTOC_SHA256  $tmp/protoc.zip" | sha256sum -c --quiet - || die "protoc checksum mismatch"
  rm -rf "$PROTOC_DIR"
  mkdir -p "$PROTOC_DIR"
  unzip -q "$tmp/protoc.zip" -d "$PROTOC_DIR"
  chmod 0755 "$bin"
  rm -rf "$tmp"
}

# Keep source locations stable across checkouts, Cargo homes and image builds.
# Preserve caller-supplied compiler flags before appending the canonical maps.
build_release() {
  local -a release_flags=()
  if [[ ${CARGO_ENCODED_RUSTFLAGS+x} ]]; then
    IFS=$'\x1f' read -r -d '' -a release_flags < <(printf '%s\0' "$CARGO_ENCODED_RUSTFLAGS")
  elif [[ -n "${RUSTFLAGS:-}" ]]; then
    read -r -d '' -a release_flags < <(printf '%s\0' "$RUSTFLAGS")
  fi
  release_flags+=(
    "--remap-path-prefix=$ROOT=/fyrr"
    "--remap-path-prefix=$(realpath -m -- "${CARGO_TARGET_DIR:-$ROOT/target}")=/target"
    "--remap-path-prefix=${CARGO_HOME:-$HOME/.cargo}=/cargo"
    "--remap-path-prefix=${RUSTUP_HOME:-$HOME/.rustup}=/rustup"
  )
  local release_encoded
  printf -v release_encoded '%s\x1f' "${release_flags[@]}"
  CARGO_ENCODED_RUSTFLAGS="${release_encoded%$'\x1f'}" \
    cargo build --locked --release "$@"
}

BUDGETS="$ROOT/budgets.toml"

# budget <section> <key>: the value in budgets.toml (empty when missing).
budget() {
  awk -v section="$1" -v key="$2" '
    /^\[/ { current = $0; gsub(/[\[\] ]/, "", current) }
    current == section && $1 == key { print $3; exit }
  ' "$BUDGETS"
}

# set_budget <section> <key> <value>: records a measured value in place,
# keeping every comment and other section.
set_budget() {
  local tmp
  tmp="$(mktemp)"
  awk -v section="$1" -v key="$2" -v value="$3" '
    /^\[/ { current = $0; gsub(/[\[\] ]/, "", current) }
    current == section && $1 == key { print key " = " value; found = 1; next }
    { print }
    END { if (!found) exit 3 }
  ' "$BUDGETS" >"$tmp" || { rm -f "$tmp"; die "budgets.toml has no [$1] $2"; }
  mv "$tmp" "$BUDGETS"
}

# compare_budget <section> <key> <measured>: prints the verdict; returns 1 on
# growth (record an intended increase with --record, by its measured delta).
compare_budget() {
  local limit
  limit="$(budget "$1" "$2")"
  [[ -n "$limit" ]] || die "budgets.toml has no [$1] $2"
  if (($3 > limit)); then
    echo "FAIL $1.$2: measured $3 > budget $limit (+$(($3 - limit))); record it in budgets.toml if intended"
    return 1
  elif (($3 < limit)); then
    echo "ok   $1.$2: measured $3 < budget $limit (-$((limit - $3))); consider lowering the budget"
  else
    echo "ok   $1.$2: measured $3 = budget $limit"
  fi
}
