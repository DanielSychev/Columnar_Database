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
    -DBUILD_TESTING=OFF
)

# Only add -march=native on x86_64 hosts. Some Docker hosts (or base images)
# use different architectures where -march=native is invalid and will break
# the configure or build step.
ARCH="$(uname -m)"
MARCH_FLAGS=""
if [ "${DISABLE_MARCH_NATIVE:-0}" != "1" ] && [ "$ARCH" = "x86_64" ]; then
    MARCH_FLAGS="-march=native"
fi

RELEASE_C_FLAGS="-O3 ${MARCH_FLAGS} -DNDEBUG"
RELEASE_CXX_FLAGS="-O3 ${MARCH_FLAGS} -DNDEBUG"

CMAKE_ARGS+=(
    -DCMAKE_C_FLAGS_RELEASE="$RELEASE_C_FLAGS"
    -DCMAKE_CXX_FLAGS_RELEASE="$RELEASE_CXX_FLAGS"
)

if command -v ninja >/dev/null 2>&1; then
    CMAKE_ARGS+=(-G Ninja)
fi

cmake "${CMAKE_ARGS[@]}"
cmake --build "$BUILD_DIR" --parallel "$JOBS" --target engine_cli clickbench
