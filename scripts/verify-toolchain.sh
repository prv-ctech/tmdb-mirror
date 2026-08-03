#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/stress-common.sh"

rust_image='rust:1.97-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa'
rust_version="$(docker_checked run --rm "$rust_image" rustc --version)"
[[ "$rust_version" == rustc\ 1.97.1\ * ]] || die 'pinned Rust image returned an unexpected compiler version'
grep -Fq 'channel = "1.97.1"' "$REPO_ROOT/rust-toolchain.toml" || die 'rust-toolchain.toml drifted'
[[ -f "$REPO_ROOT/Cargo.lock" ]] || die 'Cargo.lock is missing'
docker_checked run --rm \
    --mount "type=bind,source=$(docker_path "$REPO_ROOT"),target=/workspace" \
    --workdir /workspace "$rust_image" \
    cargo metadata --locked --no-deps --format-version 1 >/dev/null
printf 'Pinned toolchain verified: %s\n' "$rust_version"
