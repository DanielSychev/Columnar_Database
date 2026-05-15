#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <input_csv> <output_columnar>" >&2
    exit 1
fi

INPUT_CSV="$1"
COLUMNAR="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCHEMA="$SCRIPT_DIR/schema_clickbench.csv"
CLI="$REPO_DIR/build/src/engine/engine_cli"

if [ ! -x "$CLI" ]; then
    echo "engine_cli was not found at $CLI; run ./script/build.sh first" >&2
    exit 1
fi

mkdir -p "$(dirname "$COLUMNAR")"
exec "$CLI" 0 "$SCHEMA" "$INPUT_CSV" "$COLUMNAR"
