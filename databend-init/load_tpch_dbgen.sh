#!/bin/bash
# Load TPC-H data into Databend using official dbgen
# Based on: https://www.databend.com/blog/category-engineering/2022-08-08-benchmark-tpc-h
#
# Usage:
#   ./load_tpch_dbgen.sh [scale_factor] [database] [host] [port]
#
# Examples:
#   ./load_tpch_dbgen.sh 1           # SF1 (1GB) into tpch1g
#   ./load_tpch_dbgen.sh 10 tpch10g  # SF10 (10GB) into tpch10g

set -e

SCALE_FACTOR=${1:-1}
DATABASE=${2:-tpch1g}
HOST=${3:-localhost}
PORT=${4:-8000}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/tpch-data/sf${SCALE_FACTOR}"

echo "TPC-H Data Loader (SF${SCALE_FACTOR})"
echo "========================================"
echo "Database: ${DATABASE}"
echo "Host: ${HOST}:${PORT}"
echo "Data dir: ${DATA_DIR}"
echo "========================================"

# Step 1: Generate data using native dbgen
generate_data() {
    echo ""
    echo "Step 1: Generating TPC-H data (SF${SCALE_FACTOR})..."

    mkdir -p "${DATA_DIR}"

    # Check if data already exists
    if [ -f "${DATA_DIR}/lineitem.tbl" ]; then
        echo "  Data files already exist in ${DATA_DIR}"
        echo "  Delete them to regenerate, or use existing files."
        return 0
    fi

    # Use native dbgen (in tpch-dbgen directory)
    DBGEN_DIR="${SCRIPT_DIR}/tpch-data/tpch-dbgen"

    if [ ! -x "${DBGEN_DIR}/dbgen" ]; then
        echo "  Building dbgen..."
        (cd "${DBGEN_DIR}" && make -f makefile.suite)
    fi

    if [ ! -x "${DBGEN_DIR}/dbgen" ]; then
        echo "  ERROR: dbgen not found at ${DBGEN_DIR}/dbgen"
        echo "  Please build it: cd ${DBGEN_DIR} && make -f makefile.suite"
        exit 1
    fi

    echo "  Running dbgen -s ${SCALE_FACTOR}..."
    (cd "${DBGEN_DIR}" && ./dbgen -vf -s ${SCALE_FACTOR})

    # Move generated files to data directory
    mv "${DBGEN_DIR}"/*.tbl "${DATA_DIR}/" 2>/dev/null || true

    # Verify files were created
    echo "  Generated files:"
    ls -lh "${DATA_DIR}"/*.tbl 2>/dev/null || echo "  ERROR: No .tbl files found!"
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

    # Check if lineitem table already exists
    EXISTS=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"SHOW TABLES FROM ${DATABASE} LIKE 'lineitem'\"}" | grep -c '"lineitem"' || true)

    if [ "${EXISTS}" = "0" ]; then
        echo "  Creating TPC-H tables in ${DATABASE}..."
        # Check if tpch1g exists as a schema source
        HAS_SRC=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d '{"sql": "SHOW TABLES FROM tpch1g LIKE '\''lineitem'\''"}' | grep -c '"lineitem"' || true)

        if [ "${HAS_SRC}" != "0" ]; then
            for t in region nation part supplier partsupp customer orders lineitem; do
                _run_sql "CREATE TABLE IF NOT EXISTS ${DATABASE}.${t} LIKE tpch1g.${t}"
            done
            echo "  Tables cloned from tpch1g"
        else
            echo "  WARNING: tpch1g schema not found. Run 'docker compose up -d' first to create base schemas."
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

    # Tables in dependency order
    TABLES="region nation supplier part partsupp customer orders lineitem"

    for t in ${TABLES}; do
        if [ ! -f "${t}.tbl" ]; then
            echo "  ${t}: SKIPPED (file not found)"
            continue
        fi

        SIZE=$(ls -lh "${t}.tbl" | awk '{print $5}')
        echo -n "  ${t} (${SIZE})... "

        START=$(date +%s.%N)

        # Truncate table first
        curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"TRUNCATE TABLE ${DATABASE}.${t}\"}" > /dev/null

        # Upload file to user stage
        UPLOAD_RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/upload_to_stage" \
            -X PUT \
            -H "stage_name:~" \
            -F "upload=@${t}.tbl")

        UPLOAD_STATE=$(echo "${UPLOAD_RESULT}" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
        if [ "${UPLOAD_STATE}" != "SUCCESS" ]; then
            echo "UPLOAD FAILED"
            echo "    ${UPLOAD_RESULT}"
            continue
        fi

        # COPY INTO from stage
        COPY_SQL="COPY INTO ${DATABASE}.${t} FROM @~/${t}.tbl FILE_FORMAT = (type = CSV field_delimiter = '|' record_delimiter = '\\n') PURGE = TRUE"

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

    # Expected counts for SF1
    declare -A EXPECTED
    EXPECTED[region]=5
    EXPECTED[nation]=25
    EXPECTED[supplier]=10000
    EXPECTED[part]=200000
    EXPECTED[partsupp]=800000
    EXPECTED[customer]=150000
    EXPECTED[orders]=1500000
    EXPECTED[lineitem]=6001215

    TABLES="region nation supplier part partsupp customer orders lineitem"

    for t in ${TABLES}; do
        RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"SELECT COUNT(*) FROM ${DATABASE}.${t}\"}")

        COUNT=$(echo "${RESULT}" | grep -o '"data":\[\["[0-9]*"\]\]' | grep -o '[0-9]*')

        if [ "${SCALE_FACTOR}" = "1" ]; then
            EXP=${EXPECTED[$t]}
            if [ "${COUNT}" = "${EXP}" ]; then
                STATUS="OK"
            else
                STATUS="MISMATCH (expected ${EXP})"
            fi
        else
            STATUS=""
        fi

        printf "  %-12s %'12d %s\n" "${t}:" "${COUNT:-0}" "${STATUS}"
    done
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
