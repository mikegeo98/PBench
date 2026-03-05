#!/bin/bash
# Load TPC-DS data into Databend
#
# This script generates TPC-DS data using dsdgen and loads it into Databend.
# Requires dsdgen to be built first (run setup_tpcds_dsdgen.sh).
#
# Usage:
#   ./load_tpcds_dbgen.sh [scale_factor] [database] [host] [port]
#
# Examples:
#   ./load_tpcds_dbgen.sh 1 tpcds1g               # SF1 into tpcds1g
#   ./load_tpcds_dbgen.sh 1 tpcds1g localhost 8000

set -e

SCALE_FACTOR=${1:-1}
DATABASE=${2:-tpcds1g}
HOST=${3:-localhost}
PORT=${4:-8000}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DSDGEN_DIR="${SCRIPT_DIR}/tpcds-data/tpcds-dsdgen/tools"
DATA_DIR="${SCRIPT_DIR}/tpcds-data/sf${SCALE_FACTOR}"

echo "TPC-DS Databend Data Loader (SF${SCALE_FACTOR})"
echo "========================================"
echo "Database: ${DATABASE}"
echo "Host: ${HOST}:${PORT}"
echo "Data dir: ${DATA_DIR}"
echo "========================================"

# TPC-DS tables in load order (dimensions before facts)
DIMENSION_TABLES="call_center catalog_page customer_address customer_demographics date_dim household_demographics income_band item promotion reason ship_mode store time_dim warehouse web_page web_site customer"
FACT_TABLES="inventory store_sales store_returns catalog_sales catalog_returns web_sales web_returns"
ALL_TABLES="${DIMENSION_TABLES} ${FACT_TABLES}"

# Step 1: Generate data if needed
generate_data() {
    echo ""
    echo "Step 1: Generating TPC-DS data (SF${SCALE_FACTOR})..."

    if [ ! -x "${DSDGEN_DIR}/dsdgen" ]; then
        echo "  ERROR: dsdgen not found at ${DSDGEN_DIR}/dsdgen"
        echo "  Please run setup_tpcds_dsdgen.sh first"
        exit 1
    fi

    mkdir -p "${DATA_DIR}"

    # Check if data already exists
    if [ -f "${DATA_DIR}/store_sales.dat" ]; then
        echo "  Data files already exist in ${DATA_DIR}"
        echo "  Delete them to regenerate."
        return 0
    fi

    echo "  Running dsdgen -scale ${SCALE_FACTOR}..."
    cd "${DSDGEN_DIR}"
    ./dsdgen -SCALE ${SCALE_FACTOR} -DIR "${DATA_DIR}" -VERBOSE Y

    echo "  Generated files:"
    ls -lh "${DATA_DIR}"/*.dat 2>/dev/null | head -10
    echo "  ..."
}

# Step 1.5: Create database and tables if they don't exist
create_database() {
    echo ""
    echo "Step 1.5: Creating database ${DATABASE} (if needed)..."

    _run_sql() {
        curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$1")}" > /dev/null
    }

    _run_sql "CREATE DATABASE IF NOT EXISTS ${DATABASE}"

    # Check if store_sales table already exists
    EXISTS=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"SHOW TABLES FROM ${DATABASE} LIKE 'store_sales'\"}" | grep -c '"store_sales"' || true)

    if [ "${EXISTS}" = "0" ]; then
        echo "  Creating TPC-DS tables in ${DATABASE}..."
        HAS_SRC=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d '{"sql": "SHOW TABLES FROM tpcds1g LIKE '\''store_sales'\''"}' | grep -c '"store_sales"' || true)

        if [ "${HAS_SRC}" != "0" ]; then
            for t in ${ALL_TABLES}; do
                _run_sql "CREATE TABLE IF NOT EXISTS ${DATABASE}.${t} LIKE tpcds1g.${t}"
            done
            echo "  Tables cloned from tpcds1g"
        else
            echo "  WARNING: tpcds1g schema not found. Run 'docker compose up -d' first to create base schemas."
            echo "  Alternatively, run: cd databend-init && python run_ddl.py"
            exit 1
        fi
    else
        echo "  Tables already exist"
    fi
}

# Step 2: Load data into Databend
load_data() {
    echo ""
    echo "Step 2: Loading data into Databend..."

    cd "${DATA_DIR}"

    for t in ${ALL_TABLES}; do
        DAT_FILE="${t}.dat"
        if [ ! -f "${DAT_FILE}" ]; then
            echo "  ${t}: SKIPPED (file not found)"
            continue
        fi

        SIZE=$(ls -lh "${DAT_FILE}" | awk '{print $5}')
        echo -n "  ${t} (${SIZE})... "

        START=$(date +%s.%N)

        # Truncate table first
        curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"TRUNCATE TABLE ${DATABASE}.${t}\"}" > /dev/null 2>&1 || true

        # Upload file to user stage
        UPLOAD_RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/upload_to_stage" \
            -X PUT \
            -H "stage_name:~" \
            -F "upload=@${DAT_FILE}")

        UPLOAD_STATE=$(echo "${UPLOAD_RESULT}" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
        if [ "${UPLOAD_STATE}" != "SUCCESS" ]; then
            echo "UPLOAD FAILED"
            echo "    ${UPLOAD_RESULT}"
            continue
        fi

        # COPY INTO from stage (TPC-DS uses | delimiter with trailing |)
        COPY_SQL="COPY INTO ${DATABASE}.${t} FROM @~/${DAT_FILE} FILE_FORMAT = (type = CSV field_delimiter = '|' record_delimiter = '\\n') PURGE = TRUE"

        COPY_RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"${COPY_SQL}\"}")

        END=$(date +%s.%N)
        ELAPSED=$(echo "$END - $START" | bc)

        # Check for errors
        ERROR=$(echo "${COPY_RESULT}" | grep -o '"error":{[^}]*}' | head -1)
        if [ -n "${ERROR}" ] && [ "${ERROR}" != '"error":null' ]; then
            echo "FAILED"
            echo "    ${ERROR}"
        else
            # Get rows from stats
            ROWS=$(echo "${COPY_RESULT}" | grep -o '"rows":[0-9]*' | grep -o '[0-9]*' | head -1)
            echo "OK (${ROWS:-?} rows in ${ELAPSED}s)"
        fi
    done
}

# Step 3: Verify row counts
verify_data() {
    echo ""
    echo "Step 3: Verifying row counts..."

    TOTAL=0
    for t in ${ALL_TABLES}; do
        RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"SELECT COUNT(*) FROM ${DATABASE}.${t}\"}")

        COUNT=$(echo "${RESULT}" | grep -o '"data":\[\["[0-9]*"\]\]' | grep -o '[0-9]*')

        if [ -n "${COUNT}" ]; then
            printf "  %-25s %'12d\n" "${t}:" "${COUNT}"
            TOTAL=$((TOTAL + COUNT))
        fi
    done

    echo ""
    printf "  %-25s %'12d\n" "TOTAL:" "${TOTAL}"
}

# Main
echo ""
generate_data
create_database
load_data
verify_data

echo ""
echo "========================================"
echo "Done!"
echo ""
echo "To use with collect.py:"
echo "  python collect.py tpcds --databend"
