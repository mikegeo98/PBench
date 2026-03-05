#!/bin/bash
# Load TPC-H SF20 and TPC-DS SF20 into Databend from MinIO
# Prerequisites: pbench-databend and pbench-minio containers running
# The minio_tpch stage already exists from a previous session.
#
# Usage: bash load_sf20_databend.sh

set -e

DATABEND="http://localhost:8000/v1/query"
MINIO_ENDPOINT="http://172.17.0.1:9000"
WAIT=600  # seconds to wait for large COPY operations

run_sql() {
    local sql="$1"
    local wait_secs="${2:-$WAIT}"
    local result
    result=$(curl -s "$DATABEND" -u root: -H "Content-Type: application/json" --max-time "$wait_secs" \
        -d "{\"sql\": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$sql"), \"pagination\": {\"wait_time_secs\": $wait_secs}}")

    local state
    state=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',''))" 2>/dev/null)

    # If running, poll the next_uri until done
    while [ "$state" = "Running" ]; do
        local next_uri
        next_uri=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('next_uri',''))" 2>/dev/null)
        if [ -z "$next_uri" ]; then break; fi
        sleep 2
        result=$(curl -s "http://localhost:8000${next_uri}" -u root: --max-time "$wait_secs")
        state=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',''))" 2>/dev/null)
    done

    local error
    error=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); e=d.get('error'); print(e['message'] if isinstance(e,dict) else (e or ''))" 2>/dev/null)
    if [ -n "$error" ] && [ "$error" != "None" ] && [ "$error" != "" ]; then
        echo "ERROR: $error" >&2
        return 1
    fi
    echo "$result"
}

get_count() {
    local db="$1" table="$2"
    curl -s "$DATABEND" -u root: -H "Content-Type: application/json" --max-time 120 \
        -d "{\"sql\": \"SELECT COUNT(*) FROM ${db}.${table}\"}" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('data', [['?']])[0][0])" 2>/dev/null
}

clone_and_load() {
    local src_db="$1" dst_db="$2" stage="$3" ext="$4"
    shift 4
    local tables=("$@")

    echo ""
    echo "=========================================="
    echo "Loading $dst_db from stage @$stage"
    echo "=========================================="

    echo ""
    echo "--- Creating database $dst_db ---"
    run_sql "CREATE DATABASE IF NOT EXISTS $dst_db" 10 > /dev/null

    echo ""
    echo "--- Creating stage @$stage ---"
    run_sql "CREATE OR REPLACE STAGE $stage URL = 's3://${stage#minio_}/sf20/' CONNECTION = (ENDPOINT_URL = '$MINIO_ENDPOINT', ACCESS_KEY_ID = 'minioadmin', SECRET_ACCESS_KEY = 'minioadmin')" 10 > /dev/null

    echo ""
    echo "--- Cloning schema from $src_db ---"
    for t in "${tables[@]}"; do
        local ddl
        ddl=$(curl -s "$DATABEND" -u root: -H "Content-Type: application/json" --max-time 10 \
            -d "{\"sql\": \"SHOW CREATE TABLE ${src_db}.${t}\"}" | \
            python3 -c "
import sys,json
d=json.load(sys.stdin)
ddl=d['data'][0][1]
ddl=ddl.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ${dst_db}.', 1)
ddl=ddl.replace(' ENGINE=FUSE', '')
print(ddl)" 2>/dev/null)
        run_sql "$ddl" 10 > /dev/null && echo "  $t: OK" || echo "  $t: FAILED"
    done

    echo ""
    echo "--- Loading data ---"
    for t in "${tables[@]}"; do
        echo -n "  $t... "
        local start end elapsed
        start=$(date +%s)

        run_sql "COPY INTO ${dst_db}.${t} FROM @${stage}/${t}.${ext} FILE_FORMAT = (type = CSV, field_delimiter = '|', record_delimiter = '\n') FORCE = TRUE" "$WAIT" > /dev/null 2>&1

        end=$(date +%s)
        elapsed=$((end - start))
        local count
        count=$(get_count "$dst_db" "$t")
        echo "$count rows (${elapsed}s)"
    done

    echo ""
    echo "--- Verifying ---"
    local total=0
    for t in "${tables[@]}"; do
        local count
        count=$(get_count "$dst_db" "$t")
        printf "  %-25s %'12s\n" "$t:" "$count"
        total=$((total + count))
    done
    printf "  %-25s %'12s\n" "TOTAL:" "$total"
}

# ======== TPC-H SF20 ========
TPCH_TABLES=(region nation supplier part partsupp customer orders lineitem)
clone_and_load "tpch1g" "tpch20g" "minio_tpch" "tbl" "${TPCH_TABLES[@]}"

# ======== TPC-DS SF20 ========
TPCDS_TABLES=(
    call_center catalog_page customer_address customer_demographics
    date_dim household_demographics income_band item promotion reason
    ship_mode store time_dim warehouse web_page web_site customer
    inventory store_sales store_returns catalog_sales catalog_returns
    web_sales web_returns
)
clone_and_load "tpcds1g" "tpcds20g" "minio_tpcds" "dat" "${TPCDS_TABLES[@]}"

echo ""
echo "=========================================="
echo "All done! Run benchmarks with:"
echo "  cd src/Collect_metrics"
echo "  python collect.py tpch20 --repeat 1 --timeout 300"
echo "  python collect.py tpcds20 --repeat 1 --timeout 300"
echo "=========================================="
