#!/usr/bin/env bash
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
    echo "script/setup.sh currently supports apt-get-based systems only" >&2
    exit 1
fi

if [ "$(id -u)" -eq 0 ]; then
    RUN_AS_ROOT=()
elif command -v sudo >/dev/null 2>&1; then
    RUN_AS_ROOT=(sudo)
else
    echo "root privileges or sudo are required to install build dependencies" >&2
    exit 1
fi

run_root() {
    "${RUN_AS_ROOT[@]}" "$@"
}

export DEBIAN_FRONTEND=noninteractive
run_root apt-get update
run_root apt-get install -y --no-install-recommends build-essential cmake ninja-build
