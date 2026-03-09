#!/bin/bash
# Load a large .tbl file into Databend in chunks (splits by line count).
# Needed because Databend's upload API can't handle files >~2GB reliably.
#
# Usage:
#   ./load_chunked.sh <database> <table> <tbl_file> [lines_per_chunk] [host] [port]
#
# Examples:
#   ./load_chunked.sh tpch20g lineitem tpch-data/sf20/lineitem.tbl
#   ./load_chunked.sh tpch20g orders tpch-data/sf20/orders.tbl 3000000

set -e

DATABASE=${1:?Usage: $0 <database> <table> <tbl_file> [lines_per_chunk] [host] [port]}
TABLE=${2:?Usage: $0 <database> <table> <tbl_file>}
TBL_FILE=${3:?Usage: $0 <database> <table> <tbl_file>}
LINES=${4:-5000000}
HOST=${5:-localhost}
PORT=${6:-8000}

if [ ! -f "$TBL_FILE" ]; then
    echo "ERROR: File not found: $TBL_FILE"
    exit 1
fi

FILE_SIZE=$(ls -lh "$TBL_FILE" | awk '{print $5}')
echo "Loading ${DATABASE}.${TABLE} from ${TBL_FILE} (${FILE_SIZE})"
echo "  Chunk size: ${LINES} lines"
echo "  Host: ${HOST}:${PORT}"
echo "========================================"

# Create temp dir for chunks
CHUNK_DIR=$(mktemp -d)
trap "rm -rf $CHUNK_DIR" EXIT

echo "Splitting into chunks..."
split -l "$LINES" --numeric-suffixes "$TBL_FILE" "${CHUNK_DIR}/chunk_"

CHUNK_COUNT=$(ls "${CHUNK_DIR}"/chunk_* | wc -l)
echo "  Created ${CHUNK_COUNT} chunks"

# Truncate table
echo "Truncating ${DATABASE}.${TABLE}..."
curl -s -u root: "http://${HOST}:${PORT}/v1/query" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"TRUNCATE TABLE ${DATABASE}.${TABLE}\"}" > /dev/null

# Load each chunk
LOADED=0
FAILED=0
for f in "${CHUNK_DIR}"/chunk_*; do
    LOADED=$((LOADED + 1))
    FNAME=$(basename "$f")
    echo -n "  [${LOADED}/${CHUNK_COUNT}] ${FNAME}... "

    # Upload to stage
    UPLOAD=$(curl -s -u root: "http://${HOST}:${PORT}/v1/upload_to_stage" \
        -X PUT -H 'stage_name:~' -F "upload=@${f}")

    UPLOAD_STATE=$(echo "$UPLOAD" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
    if [ "$UPLOAD_STATE" != "SUCCESS" ]; then
        echo "UPLOAD FAILED"
        FAILED=$((FAILED + 1))
        continue
    fi

    # COPY INTO
    RESULT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query" \
        -H 'Content-Type: application/json' \
        -d "{\"sql\":\"COPY INTO ${DATABASE}.${TABLE} FROM @~/${FNAME} FILE_FORMAT = (type = CSV field_delimiter = '|' record_delimiter = '\\\\n') PURGE = TRUE\"}")

    ERROR=$(echo "$RESULT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
e=d.get('error')
print(e['message'] if e else '')
" 2>/dev/null)

    if [ -n "$ERROR" ]; then
        echo "FAILED: $ERROR"
        FAILED=$((FAILED + 1))
    else
        echo "OK"
    fi
done

# Verify
echo ""
echo "========================================"
COUNT=$(curl -s -u root: "http://${HOST}:${PORT}/v1/query" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\":\"SELECT COUNT(*) FROM ${DATABASE}.${TABLE}\"}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0][0])")

echo "Total rows in ${DATABASE}.${TABLE}: ${COUNT}"
echo "Chunks: ${CHUNK_COUNT} total, $((CHUNK_COUNT - FAILED)) succeeded, ${FAILED} failed"
