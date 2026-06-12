#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_DIR/build-o3-native/clickbench/clickbench"
OUT_DIR="$REPO_DIR/experiments"

MF_PATH="$REPO_DIR/src/TestFiles/output.mf"
RESULT_PREFIX="$REPO_DIR/src/TestFiles/result"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--mf)       MF_PATH="$2";       shift 2 ;;
        -r|--results)  RESULT_PREFIX="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; echo "Usage: $0 [-m input.mf] [-r result_prefix]" >&2; exit 1 ;;
    esac
done

if [ ! -x "$BINARY" ]; then
    echo "clickbench binary not found at $BINARY; run ./script/build.sh first" >&2
    exit 1
fi

if [ ! -f "$MF_PATH" ]; then
    echo "input file not found: $MF_PATH" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTFILE="$OUT_DIR/bench_${TIMESTAMP}.txt"

COLD_TMP=$(mktemp)
HOT_TMP=$(mktemp)
trap "rm -f '$COLD_TMP' '$HOT_TMP'" EXIT

cd "$REPO_DIR"

echo "Purging disk cache (requires sudo)..."
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

echo "Cold run..."
"$BINARY" -1 "$MF_PATH" "$RESULT_PREFIX" 2>"$COLD_TMP"

echo "Hot run..."
"$BINARY" -1 "$MF_PATH" "$RESULT_PREFIX" 2>"$HOT_TMP"

# build report
{
    echo "ClickBench cold/hot — $(date)"
    echo "Binary: $BINARY"
    echo "Input:  $MF_PATH"
    echo ""

    printf "%-7s | %-12s | %-12s\n" "Query" "Cold (ms)" "Hot (ms)"
    printf "%-7s-+-%-12s-+-%-12s\n" "-------" "------------" "------------"

    paste \
        <(grep "^Query" "$COLD_TMP" | awk '{printf "%.2f\n", $4 * 1000}') \
        <(grep "^Query" "$HOT_TMP"  | awk '{printf "%.2f\n", $4 * 1000}') \
    | awk 'BEGIN{i=0} {
        printf "%-7d | %-12s | %-12s\n", i, $1, $2
        i++
    }'

    printf "%-7s-+-%-12s-+-%-12s\n" "-------" "------------" "------------"

    stats() {
        grep "^Query" "$1" | awk '{print $4}' | awk '
            BEGIN { s=0; logsum=0; max=0; n=0 }
            {
                s += $1; logsum += log($1); n++
                if ($1 > max) max = $1
            }
            END {
                printf "%.2f %.2f %.2f", exp(logsum/n)*1000, s*1000, max*1000
            }'
    }

    read -r geomean_cold sum_cold max_cold <<< "$(stats "$COLD_TMP")"
    read -r geomean_hot  sum_hot  max_hot  <<< "$(stats "$HOT_TMP")"

    printf "%-7s | %-12s | %-12s\n" "Geomean" "$geomean_cold" "$geomean_hot"
    printf "%-7s | %-12s | %-12s\n" "Sum"     "$sum_cold"     "$sum_hot"
    printf "%-7s | %-12s | %-12s\n" "Max"     "$max_cold"     "$max_hot"
} | tee "$OUTFILE"

echo ""
echo "Written to: $OUTFILE"
