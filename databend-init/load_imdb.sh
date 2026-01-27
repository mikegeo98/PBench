#!/bin/bash
# Load IMDB/JOB (Join Order Benchmark) data into Databend
# Data source: http://homepages.cwi.nl/~boncz/job/imdb.tgz
#
# Usage:
#   ./load_imdb.sh [database] [host] [port]
#
# Examples:
#   ./load_imdb.sh imdb                 # Load into 'imdb' database
#   ./load_imdb.sh imdb localhost 8000  # Specify host/port

set -e

DATABASE=${1:-imdb}
HOST=${2:-localhost}
PORT=${3:-8000}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/imdb-data"

# Multiple download sources (try in order)
ARCHIVE_URLS=(
    "https://event.cwi.nl/da/job/imdb.tgz"      # Original CWI source
    "https://bonsai.cedardb.com/job/imdb.tgz"   # CedarDB mirror
)

echo "IMDB/JOB Data Loader"
echo "========================================"
echo "Database: ${DATABASE}"
echo "Host: ${HOST}:${PORT}"
echo "Data dir: ${DATA_DIR}"
echo "========================================"

# Step 1: Download and extract data
download_data() {
    echo ""
    echo "Step 1: Downloading IMDB data..."

    mkdir -p "${DATA_DIR}"
    cd "${DATA_DIR}"

    # Check if data already exists
    if [ -f "${DATA_DIR}/title.csv" ]; then
        echo "  Data files already exist in ${DATA_DIR}"
        echo "  Delete them to re-download, or use existing files."
        return 0
    fi

    # Download the archive (try multiple sources)
    ARCHIVE_FILE="${DATA_DIR}/imdb.tgz"
    if [ ! -f "${ARCHIVE_FILE}" ] || [ ! -s "${ARCHIVE_FILE}" ]; then
        rm -f "${ARCHIVE_FILE}"  # Remove if empty/corrupt

        for url in "${ARCHIVE_URLS[@]}"; do
            echo "  Trying: ${url}..."
            if curl -fL -o "${ARCHIVE_FILE}" "${url}"; then
                # Verify it's a valid gzip file
                if gzip -t "${ARCHIVE_FILE}" 2>/dev/null; then
                    echo "  Download successful!"
                    break
                else
                    echo "  Downloaded file is not valid gzip, trying next source..."
                    rm -f "${ARCHIVE_FILE}"
                fi
            else
                echo "  Failed to download from ${url}"
            fi
        done

        if [ ! -f "${ARCHIVE_FILE}" ]; then
            echo "  ERROR: Could not download IMDB data from any source!"
            echo "  You can manually download from: https://event.cwi.nl/da/job/imdb.tgz"
            echo "  Place the file at: ${ARCHIVE_FILE}"
            exit 1
        fi
    fi

    # Extract
    echo "  Extracting archive (~1.2GB compressed, ~3.7GB extracted)..."
    tar -xzf "${ARCHIVE_FILE}" -C "${DATA_DIR}"

    # The archive contains a subdirectory, move files up if needed
    if [ -d "${DATA_DIR}/imdb" ]; then
        mv "${DATA_DIR}/imdb"/* "${DATA_DIR}/"
        rmdir "${DATA_DIR}/imdb"
    fi

    echo "  Downloaded files:"
    ls -lh "${DATA_DIR}"/*.csv 2>/dev/null || ls -lh "${DATA_DIR}"/*.txt 2>/dev/null || echo "  Files extracted"
}

# Step 2: Create database and tables
create_schema() {
    echo ""
    echo "Step 2: Creating database and tables..."

    # Create database
    curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"CREATE DATABASE IF NOT EXISTS ${DATABASE}\"}" > /dev/null

    echo "  Created database: ${DATABASE}"

    # Read schema file
    SCHEMA_FILE="${SCRIPT_DIR}/../src/PBench-tool/LLM_tools/input/table_schema/imdb.sql"
    if [ ! -f "${SCHEMA_FILE}" ]; then
        echo "  ERROR: Schema file not found: ${SCHEMA_FILE}"
        echo "  Creating inline schema..."
        create_inline_schema
        return
    fi

    # Execute each CREATE TABLE statement
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip comments and empty lines
        [[ "$line" =~ ^[[:space:]]*-- ]] && continue
        [[ -z "$line" ]] && continue

        # Accumulate lines until we hit a semicolon
        SQL_BUFFER="${SQL_BUFFER}${line} "

        if [[ "$line" == *";"* ]]; then
            # Extract table name for logging
            TABLE_NAME=$(echo "${SQL_BUFFER}" | grep -oP 'CREATE TABLE \K\w+' | head -1)

            # Add database prefix and execute
            SQL_WITH_DB="${SQL_BUFFER/CREATE TABLE /CREATE TABLE IF NOT EXISTS ${DATABASE}.}"

            RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
                -H "Content-Type: application/json" \
                -d "{\"sql\": \"${SQL_WITH_DB}\"}")

            ERROR=$(echo "${RESULT}" | grep -o '"error":{[^}]*}' | head -1)
            if [ -n "${ERROR}" ] && [ "${ERROR}" != '"error":null' ] && ! echo "${ERROR}" | grep -q "already exists"; then
                echo "  ${TABLE_NAME}: FAILED - ${ERROR}"
            else
                echo "  ${TABLE_NAME}: OK"
            fi

            SQL_BUFFER=""
        fi
    done < "${SCHEMA_FILE}"
}

create_inline_schema() {
    # Inline schema definitions if file not found
    TABLES=(
        "aka_name:id INT, person_id INT, name VARCHAR, imdb_index VARCHAR(12), name_pcode_cf VARCHAR(5), name_pcode_nf VARCHAR(5), surname_pcode VARCHAR(5), md5sum VARCHAR(32)"
        "aka_title:id INT, movie_id INT, title VARCHAR, imdb_index VARCHAR(12), kind_id INT, production_year INT, phonetic_code VARCHAR(5), episode_of_id INT, season_nr INT, episode_nr INT, note VARCHAR, md5sum VARCHAR(32)"
        "cast_info:id INT, person_id INT, movie_id INT, person_role_id INT, note VARCHAR, nr_order INT, role_id INT"
        "char_name:id INT, name VARCHAR, imdb_index VARCHAR(12), imdb_id INT, name_pcode_nf VARCHAR(5), surname_pcode VARCHAR(5), md5sum VARCHAR(32)"
        "comp_cast_type:id INT, kind VARCHAR(32)"
        "company_name:id INT, name VARCHAR, country_code VARCHAR(255), imdb_id INT, name_pcode_nf VARCHAR(5), name_pcode_sf VARCHAR(5), md5sum VARCHAR(32)"
        "company_type:id INT, kind VARCHAR(32)"
        "complete_cast:id INT, movie_id INT, subject_id INT, status_id INT"
        "info_type:id INT, info VARCHAR(32)"
        "keyword:id INT, keyword VARCHAR, phonetic_code VARCHAR(5)"
        "kind_type:id INT, kind VARCHAR(15)"
        "link_type:id INT, link VARCHAR(32)"
        "movie_companies:id INT, movie_id INT, company_id INT, company_type_id INT, note VARCHAR"
        "movie_info:id INT, movie_id INT, info_type_id INT, info VARCHAR, note VARCHAR"
        "movie_info_idx:id INT, movie_id INT, info_type_id INT, info VARCHAR, note VARCHAR"
        "movie_keyword:id INT, movie_id INT, keyword_id INT"
        "movie_link:id INT, movie_id INT, linked_movie_id INT, link_type_id INT"
        "name:id INT, name VARCHAR, imdb_index VARCHAR(12), imdb_id INT, gender VARCHAR(1), name_pcode_cf VARCHAR(5), name_pcode_nf VARCHAR(5), surname_pcode VARCHAR(5), md5sum VARCHAR(32)"
        "person_info:id INT, person_id INT, info_type_id INT, info VARCHAR, note VARCHAR"
        "role_type:id INT, role VARCHAR(32)"
        "title:id INT, title VARCHAR, imdb_index VARCHAR(12), kind_id INT, production_year INT, imdb_id INT, phonetic_code VARCHAR(5), episode_of_id INT, season_nr INT, episode_nr INT, series_years VARCHAR(49), md5sum VARCHAR(32)"
    )

    for table_def in "${TABLES[@]}"; do
        TABLE_NAME="${table_def%%:*}"
        COLUMNS="${table_def#*:}"

        SQL="CREATE TABLE IF NOT EXISTS ${DATABASE}.${TABLE_NAME} (${COLUMNS})"

        RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"${SQL}\"}")

        echo "  ${TABLE_NAME}: OK"
    done
}

# Step 2.5: Preprocess data files (remove trailing $ from IMDB export)
preprocess_data() {
    echo ""
    echo "Step 2.5: Preprocessing data files..."

    cd "${DATA_DIR}"

    # Check if preprocessing is needed (look for trailing $)
    # The IMDB export has $ at end of each line which breaks CSV parsing
    if tail -c 2 kind_type.csv 2>/dev/null | grep -q '\$'; then
        echo "  Removing trailing '\$' from CSV files (IMDB export quirk)..."
        for f in *.csv; do
            if [ -f "$f" ]; then
                SIZE=$(ls -lh "$f" | awk '{print $5}')
                echo -n "    ${f} (${SIZE})... "
                # Remove trailing $ from each line (use \x24 for literal $ in sed)
                sed -i 's/\x24$//' "$f"
                echo "done"
            fi
        done
        echo "  Preprocessing complete!"
    else
        echo "  Files already preprocessed or don't need preprocessing"
    fi
}

# Step 3: Load data into tables
load_data() {
    echo ""
    echo "Step 3: Loading data into Databend..."

    cd "${DATA_DIR}"

    # Tables and their corresponding data files
    # The IMDB data files are typically in CSV format
    TABLES="comp_cast_type company_type info_type kind_type link_type role_type aka_name aka_title char_name company_name keyword name title cast_info complete_cast movie_companies movie_info movie_info_idx movie_keyword movie_link person_info"

    for t in ${TABLES}; do
        # Try different file extensions
        DATA_FILE=""
        for ext in csv txt; do
            if [ -f "${t}.${ext}" ]; then
                DATA_FILE="${t}.${ext}"
                break
            fi
        done

        if [ -z "${DATA_FILE}" ]; then
            echo "  ${t}: SKIPPED (file not found)"
            continue
        fi

        SIZE=$(ls -lh "${DATA_FILE}" | awk '{print $5}')
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
            -F "upload=@${DATA_FILE}")

        UPLOAD_STATE=$(echo "${UPLOAD_RESULT}" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
        if [ "${UPLOAD_STATE}" != "SUCCESS" ]; then
            echo "UPLOAD FAILED"
            echo "    ${UPLOAD_RESULT}"
            continue
        fi

        # COPY INTO from stage - IMDB data uses comma delimiter with quoted fields
        # Use a temp file to avoid JSON escaping issues with quotes
        COPY_SQL="COPY INTO ${DATABASE}.${t} FROM @~/${DATA_FILE} FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\n', skip_header = 0, quote = '\"') PURGE = TRUE ON_ERROR = CONTINUE"

        # Write JSON to temp file to handle escaping properly
        TEMP_JSON=$(mktemp)
        cat > "${TEMP_JSON}" << EOJSON
{"sql": "COPY INTO ${DATABASE}.${t} FROM @~/${DATA_FILE} FILE_FORMAT = (type = CSV, field_delimiter = ',', record_delimiter = '\\n', skip_header = 0, quote = '\"') PURGE = TRUE ON_ERROR = CONTINUE"}
EOJSON

        COPY_RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d @"${TEMP_JSON}")
        rm -f "${TEMP_JSON}"

        END=$(date +%s.%N)
        ELAPSED=$(echo "$END - $START" | bc)

        # Check for errors
        ERROR=$(echo "${COPY_RESULT}" | grep -o '"error":{[^}]*}' | head -1)
        if [ -n "${ERROR}" ] && [ "${ERROR}" != '"error":null' ]; then
            echo "FAILED"
            echo "    ${ERROR}"
        else
            ROWS=$(echo "${COPY_RESULT}" | grep -o '"rows":[0-9]*' | grep -o '[0-9]*' | head -1)
            echo "OK (${ROWS:-?} rows in ${ELAPSED}s)"
        fi
    done
}

# Step 4: Verify row counts
verify_data() {
    echo ""
    echo "Step 4: Verifying row counts..."

    # Expected counts for standard IMDB/JOB dataset
    declare -A EXPECTED
    EXPECTED[aka_name]=901343
    EXPECTED[aka_title]=361472
    EXPECTED[cast_info]=36244344
    EXPECTED[char_name]=3140339
    EXPECTED[company_name]=234997
    EXPECTED[company_type]=4
    EXPECTED[comp_cast_type]=4
    EXPECTED[complete_cast]=135086
    EXPECTED[info_type]=113
    EXPECTED[keyword]=134170
    EXPECTED[kind_type]=7
    EXPECTED[link_type]=18
    EXPECTED[movie_companies]=2609129
    EXPECTED[movie_info]=14835720
    EXPECTED[movie_info_idx]=1380035
    EXPECTED[movie_keyword]=4523930
    EXPECTED[movie_link]=29997
    EXPECTED[name]=4167491
    EXPECTED[person_info]=2963664
    EXPECTED[role_type]=12
    EXPECTED[title]=2528312

    TABLES="comp_cast_type company_type info_type kind_type link_type role_type aka_name aka_title char_name company_name keyword name title cast_info complete_cast movie_companies movie_info movie_info_idx movie_keyword movie_link person_info"

    TOTAL=0
    for t in ${TABLES}; do
        RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query/" \
            -H "Content-Type: application/json" \
            -d "{\"sql\": \"SELECT COUNT(*) FROM ${DATABASE}.${t}\"}" 2>/dev/null)

        COUNT=$(echo "${RESULT}" | grep -o '"data":\[\["[0-9]*"\]\]' | grep -o '[0-9]*')
        COUNT=${COUNT:-0}

        EXP=${EXPECTED[$t]:-?}
        if [ "${EXP}" != "?" ] && [ "${COUNT}" = "${EXP}" ]; then
            STATUS="OK"
        elif [ "${EXP}" != "?" ] && [ "${COUNT}" != "0" ]; then
            STATUS="(expected ${EXP})"
        else
            STATUS=""
        fi

        printf "  %-18s %'12d %s\n" "${t}:" "${COUNT}" "${STATUS}"
        TOTAL=$((TOTAL + COUNT))
    done

    echo ""
    printf "  %-18s %'12d\n" "TOTAL:" "${TOTAL}"
}

# Main
echo ""
download_data
create_schema
preprocess_data
load_data
verify_data

echo ""
echo "========================================"
echo "Done!"
