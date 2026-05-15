#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$REPO_DIR/build-o3-native"

if command -v nproc >/dev/null 2>&1; then
    JOBS="$(nproc)"
else
    JOBS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
fi

mkdir -p "$BUILD_DIR"

CMAKE_ARGS=(
    -S "$REPO_DIR"
    -B "$BUILD_DIR"
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_C_FLAGS_RELEASE="-O3 -march=native -DNDEBUG"
    -DCMAKE_CXX_FLAGS_RELEASE="-O3 -march=native -DNDEBUG"
    -DBUILD_TESTING=OFF
)

if command -v ninja >/dev/null 2>&1; then
    CMAKE_ARGS+=(-G Ninja)
fi

cmake "${CMAKE_ARGS[@]}"
cmake --build "$BUILD_DIR" --parallel "$JOBS" --target engine_cli clickbench
