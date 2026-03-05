#!/bin/bash
# Load TPC-H SF100 parquet data into Databend
# Requires: parquet files mounted at /benchmark_data/tpch100/ inside the container
#
# Usage:
#   ./load_tpch100g_parquet.sh [host] [port]

set -e

HOST=${1:-localhost}
PORT=${2:-8000}

DB="tpch100g"
PARQUET_DIR="/benchmark_data/tpch100"

run_sql() {
    local sql="$1"
    local result
    result=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"$sql\"}")

    local error
    error=$(echo "$result" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    e = d.get('error')
    if e and e.get('message'):
        print(e['message'])
except: pass
" 2>/dev/null)

    if [ -n "$error" ]; then
        echo "  ERROR: $error"
        return 1
    fi
    echo "$result"
}

echo "TPC-H SF100 Parquet Loader"
echo "========================================"
echo "Database: ${DB}"
echo "Host: ${HOST}:${PORT}"
echo "Parquet dir (in container): ${PARQUET_DIR}"
echo "========================================"

# Step 1: Create database
echo ""
echo "Step 1: Creating database ${DB}..."
run_sql "CREATE DATABASE IF NOT EXISTS ${DB}" > /dev/null
echo "  Done."

# Step 2: Create tables
echo ""
echo "Step 2: Creating tables..."

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.nation (
    n_nationkey INT NOT NULL,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment VARCHAR(152)
)" > /dev/null
echo "  nation"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.region (
    r_regionkey INT NOT NULL,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152)
)" > /dev/null
echo "  region"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.part (
    p_partkey INT NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr VARCHAR(25) NOT NULL,
    p_brand VARCHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INT NOT NULL,
    p_container VARCHAR(10) NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL
)" > /dev/null
echo "  part"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.supplier (
    s_suppkey INT NOT NULL,
    s_name VARCHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone VARCHAR(15) NOT NULL,
    s_acctbal DECIMAL(15, 2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL
)" > /dev/null
echo "  supplier"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.partsupp (
    ps_partkey INT NOT NULL,
    ps_suppkey INT NOT NULL,
    ps_availqty INT NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL
)" > /dev/null
echo "  partsupp"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.customer (
    c_custkey INT NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INT NOT NULL,
    c_phone VARCHAR(15) NOT NULL,
    c_acctbal DECIMAL(15, 2) NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
)" > /dev/null
echo "  customer"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey INT NOT NULL,
    o_orderstatus VARCHAR(1) NOT NULL,
    o_totalprice DECIMAL(15, 2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk VARCHAR(15) NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment VARCHAR(79) NOT NULL
)" > /dev/null
echo "  orders"

run_sql "CREATE TABLE IF NOT EXISTS ${DB}.lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
)" > /dev/null
echo "  lineitem"

echo "  All tables created."

# Step 3: Create internal stage pointing to the parquet directory
echo ""
echo "Step 3: Creating stage for parquet files..."
run_sql "CREATE OR REPLACE STAGE ${DB}.tpch100_stage URL='fs://${PARQUET_DIR}/' FILE_FORMAT=(type=PARQUET)" > /dev/null
echo "  Stage created."

# Step 4: Load data
echo ""
echo "Step 4: Loading data from parquet files..."

TABLES="region nation supplier part partsupp customer orders lineitem"

for t in ${TABLES}; do
    # Check current row count
    COUNT_RESULT=$(run_sql "SELECT COUNT(*) FROM ${DB}.${t}")
    CURRENT_COUNT=$(echo "$COUNT_RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d['data'][0][0])
except: print('0')
" 2>/dev/null)

    if [ "$CURRENT_COUNT" != "0" ] && [ -n "$CURRENT_COUNT" ]; then
        echo "  ${t}: already has ${CURRENT_COUNT} rows, skipping (truncate to reload)"
        continue
    fi

    echo -n "  ${t}... "
    START=$(date +%s)

    LOAD_RESULT=$(run_sql "COPY INTO ${DB}.${t} FROM @${DB}.tpch100_stage/${t}.parquet FILE_FORMAT=(type=PARQUET) PURGE=FALSE")

    END=$(date +%s)
    ELAPSED=$((END - START))

    # Check result
    ERROR_CHECK=$(echo "$LOAD_RESULT" | grep -o '"error"' | head -1)
    if echo "$LOAD_RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if d.get('error') and d['error'].get('message'):
    print(d['error']['message'])
    sys.exit(1)
" 2>/dev/null; then
        # Get row count after load
        COUNT_AFTER=$(run_sql "SELECT COUNT(*) FROM ${DB}.${t}" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d['data'][0][0])
except: print('?')
" 2>/dev/null)
        echo "OK (${COUNT_AFTER} rows in ${ELAPSED}s)"
    else
        echo "FAILED (${ELAPSED}s)"
    fi
done

# Step 5: Verify
echo ""
echo "Step 5: Verifying row counts..."
printf "  %-12s %15s\n" "Table" "Rows"
printf "  %-12s %15s\n" "────────────" "───────────────"

for t in ${TABLES}; do
    COUNT=$(run_sql "SELECT COUNT(*) FROM ${DB}.${t}" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d['data'][0][0])
except: print('ERROR')
" 2>/dev/null)
    printf "  %-12s %'15d\n" "${t}" "${COUNT}"
done

echo ""
echo "========================================"
echo "Done!"
