#!/bin/bash
# Load TPC-H data into PostgreSQL
#
# This script generates TPC-H data using dbgen and loads it into PostgreSQL.
# Requires the tpch-data directory with generated .tbl files (from load_tpch_dbgen.sh).
#
# Usage:
#   ./load_tpch_postgres.sh [scale_factor] [database] [host] [port]
#
# Examples:
#   ./load_tpch_postgres.sh 1 tpch1g               # SF1 into tpch1g database
#   ./load_tpch_postgres.sh 1 tpch1g localhost 5432

set -e

SCALE=${1:-1}
DATABASE=${2:-tpch1g}
HOST=${3:-}
PORT=${4:-5432}
USER=${PGUSER:-$(whoami)}
PASSWORD=${PGPASSWORD:-}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/tpch-data/sf${SCALE}"

# Only set password if provided (peer auth doesn't need it)
[ -n "${PASSWORD}" ] && export PGPASSWORD="${PASSWORD}"

# Build connection args - omit host for local peer auth
CONN_ARGS="-p ${PORT} -U ${USER}"
[ -n "${HOST}" ] && CONN_ARGS="-h ${HOST} ${CONN_ARGS}"

echo "TPC-H PostgreSQL Data Loader"
echo "========================================"
echo "Scale Factor: ${SCALE}"
echo "Database: ${DATABASE}"
echo "Host: ${HOST:-local socket}:${PORT}"
echo "User: ${USER}"
echo "Data dir: ${DATA_DIR}"
echo "========================================"

# Check if data exists
if [ ! -f "${DATA_DIR}/lineitem.tbl" ]; then
    echo ""
    echo "ERROR: TPC-H data files not found in ${DATA_DIR}"
    echo "Please generate data first using:"
    echo "  ./load_tpch_dbgen.sh ${SCALE} databend_db"
    echo ""
    echo "This will create .tbl files that can be loaded into PostgreSQL."
    exit 1
fi

# Create database if it doesn't exist
echo ""
echo "Step 1: Creating database..."
psql ${CONN_ARGS} -d postgres -c "DROP DATABASE IF EXISTS ${DATABASE};" 2>/dev/null || true
psql ${CONN_ARGS} -d postgres -c "CREATE DATABASE ${DATABASE};"
echo "  Created database: ${DATABASE}"

# Create schema
echo ""
echo "Step 2: Creating TPC-H schema..."
psql ${CONN_ARGS} -d "${DATABASE}" << 'EOSQL'
-- TPC-H Schema for PostgreSQL

CREATE TABLE region (
    r_regionkey INTEGER NOT NULL PRIMARY KEY,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152)
);

CREATE TABLE nation (
    n_nationkey INTEGER NOT NULL PRIMARY KEY,
    n_name CHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment VARCHAR(152)
);

CREATE TABLE supplier (
    s_suppkey INTEGER NOT NULL PRIMARY KEY,
    s_name CHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL
);

CREATE TABLE part (
    p_partkey INTEGER NOT NULL PRIMARY KEY,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE customer (
    c_custkey INTEGER NOT NULL PRIMARY KEY,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
);

CREATE TABLE orders (
    o_orderkey INTEGER NOT NULL PRIMARY KEY,
    o_custkey INTEGER NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey INTEGER NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
);
EOSQL
echo "  Schema created"

# Load data
echo ""
echo "Step 3: Loading data..."

TABLES="region nation supplier part partsupp customer orders lineitem"

for t in ${TABLES}; do
    TBL_FILE="${DATA_DIR}/${t}.tbl"
    if [ ! -f "${TBL_FILE}" ]; then
        echo "  ${t}: SKIPPED (file not found)"
        continue
    fi

    SIZE=$(ls -lh "${TBL_FILE}" | awk '{print $5}')
    echo -n "  ${t} (${SIZE})... "

    START=$(date +%s.%N)

    # PostgreSQL COPY command - TPC-H .tbl files use | as delimiter with trailing |
    # Strip trailing | before importing
    sed 's/|$//' "${TBL_FILE}" | psql ${CONN_ARGS} -d "${DATABASE}" -c "\COPY ${t} FROM STDIN WITH (FORMAT csv, DELIMITER '|')" 2>/dev/null

    END=$(date +%s.%N)
    ELAPSED=$(echo "$END - $START" | bc)

    COUNT=$(psql ${CONN_ARGS} -d "${DATABASE}" -t -c "SELECT COUNT(*) FROM ${t};" | tr -d ' ')
    echo "${COUNT} rows in ${ELAPSED}s"
done

# Verify
echo ""
echo "Step 4: Verifying row counts..."

declare -A EXPECTED
EXPECTED[region]=5
EXPECTED[nation]=25
EXPECTED[supplier]=10000
EXPECTED[part]=200000
EXPECTED[partsupp]=800000
EXPECTED[customer]=150000
EXPECTED[orders]=1500000
EXPECTED[lineitem]=6001215

TOTAL=0
for t in ${TABLES}; do
    COUNT=$(psql ${CONN_ARGS} -d "${DATABASE}" -t -c "SELECT COUNT(*) FROM ${t};" | tr -d ' ')
    EXP=$((${EXPECTED[$t]} * ${SCALE}))
    if [ "${COUNT}" = "${EXP}" ]; then
        STATUS="OK"
    else
        STATUS="(expected ${EXP})"
    fi
    printf "  %-12s %'12d %s\n" "${t}:" "${COUNT}" "${STATUS}"
    TOTAL=$((TOTAL + COUNT))
done

echo ""
printf "  %-12s %'12d\n" "TOTAL:" "${TOTAL}"

# Create indexes for better query performance
echo ""
echo "Step 5: Creating indexes..."
psql ${CONN_ARGS} -d "${DATABASE}" << 'EOSQL'
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
ANALYZE;
EOSQL
echo "  Indexes created and statistics updated"

echo ""
echo "========================================"
echo "Done!"
echo ""
echo "To use with collect.py:"
echo "  export PG_PASSWORD=postgres"
echo "  python collect.py tpch --postgres --pg-database ${DATABASE}"
