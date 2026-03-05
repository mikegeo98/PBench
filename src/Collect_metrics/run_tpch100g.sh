#!/bin/bash
# Run TPC-H SF100 metrics collection on Databend
# Uses unbuffered output (-u) so you see progress in real-time.
# Timeout set to 900s (15 min) to handle heavy queries like Q3 (~11 min).
#
# Usage:
#   ./run_tpch100g.sh          # Resume from where it left off
#   ./run_tpch100g.sh --start 0  # Start from scratch (query 0)

set -e
cd "$(dirname "$0")"

echo "=== TPC-H SF100 Metrics Collection ==="
echo "Timeout: 900s per query | Repeats: 2"
echo ""

# Check if Databend is reachable
if ! curl -s -u 'root:' -o /dev/null -w '%{http_code}' \
     -H 'Content-Type: application/json' \
     --data '{"sql":"SELECT 1"}' \
     http://localhost:8000/v1/query/ | grep -q 200; then
    echo "ERROR: Databend is not reachable at localhost:8000"
    echo "Start it with: cd /home/mikeg/Documents/fork_pbench/PBench && docker compose up -d databend prometheus"
    exit 1
fi

echo "Databend is healthy. Starting collection..."
echo ""

python3 -u collect.py tpch100g --repeat 2 --timeout 900 "$@"
