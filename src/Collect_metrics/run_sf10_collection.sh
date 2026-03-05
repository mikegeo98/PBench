#!/bin/bash
# Run SF10 metrics collection for TPC-H and TPC-DS on Databend
# Uses unbuffered output (-u) so you see progress in real-time.
#
# Usage:
#   ./run_sf10_collection.sh           # Run both TPC-H and TPC-DS
#   ./run_sf10_collection.sh tpch10g   # Run only TPC-H SF10
#   ./run_sf10_collection.sh tpcds10g  # Run only TPC-DS SF10

set -e
cd "$(dirname "$0")"

# Check if Databend is reachable
if ! curl -s -u 'root:' -o /dev/null -w '%{http_code}' \
     -H 'Content-Type: application/json' \
     --data '{"sql":"SELECT 1"}' \
     http://localhost:8000/v1/query/ | grep -q 200; then
    echo "ERROR: Databend is not reachable at localhost:8000"
    echo "Start it with: cd $(dirname "$0")/../../ && docker compose up -d databend prometheus"
    exit 1
fi

echo "Databend is healthy."

run_benchmark() {
    local bench=$1
    echo ""
    echo "============================================="
    echo "  Running $bench metrics collection"
    echo "============================================="
    python3 -u collect.py "$bench" --repeat 2 --timeout 120
}

if [ $# -eq 0 ]; then
    # Run both
    run_benchmark tpch10g
    run_benchmark tpcds10g
else
    run_benchmark "$1"
fi

echo ""
echo "All done!"
