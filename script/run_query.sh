#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <query_num> <columnar> <output_csv> <log_file>" >&2
    exit 1
fi

QUERY_NUM="$1"
COLUMNAR="$2"
OUTPUT="$3"
LOGS="$4"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI="$REPO_DIR/build/clickbench/clickbench"

if [ ! -x "$CLI" ]; then
    echo "clickbench binary was not found at $CLI; run ./script/build.sh first" >&2
    exit 1
fi

mkdir -p "$(dirname "$OUTPUT")" "$(dirname "$LOGS")"
exec "$CLI" "$QUERY_NUM" "$COLUMNAR" "$OUTPUT" 2>"$LOGS"
