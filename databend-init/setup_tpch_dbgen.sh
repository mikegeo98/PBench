#!/bin/bash
# Set up TPC-H dbgen tool
#
# This downloads and builds the TPC-H data generator (dbgen).
# Uses the electrum/tpch-dbgen fork that works on Linux.
#
# Usage:
#   ./setup_tpch_dbgen.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPCH_DIR="${SCRIPT_DIR}/tpch-data"
DBGEN_DIR="${TPCH_DIR}/tpch-dbgen"

echo "TPC-H dbgen Setup"
echo "========================================"
echo "Target: ${DBGEN_DIR}"
echo "========================================"

mkdir -p "${TPCH_DIR}"

# Check if already installed
if [ -x "${DBGEN_DIR}/dbgen" ]; then
    echo ""
    echo "dbgen already installed at ${DBGEN_DIR}/dbgen"
    echo "To rebuild, remove ${DBGEN_DIR} and run again."
    exit 0
fi

# Clone tpch-dbgen
echo ""
echo "Step 1: Downloading TPC-H tools..."
if [ -d "${DBGEN_DIR}" ]; then
    echo "  Directory exists, removing..."
    rm -rf "${DBGEN_DIR}"
fi

git clone https://github.com/electrum/tpch-dbgen.git "${DBGEN_DIR}"
echo "  Downloaded to ${DBGEN_DIR}"

# Build dbgen
echo ""
echo "Step 2: Building dbgen..."
cd "${DBGEN_DIR}"

# Patch makefile.suite for Linux build
# Set MACHINE=LINUX (required for correct compilation)
# Keep DATABASE=ORACLE (only affects qgen query templates, not data generation)
cp makefile.suite makefile
sed -i 's/^CC\s*=.*/CC      = gcc/' makefile
sed -i 's/^DATABASE\s*=.*/DATABASE= ORACLE/' makefile
sed -i 's/^MACHINE\s*=.*/MACHINE = LINUX/' makefile
sed -i 's/^WORKLOAD\s*=.*/WORKLOAD = TPCH/' makefile

make dbgen 2>&1

if [ ! -x "dbgen" ]; then
    echo "ERROR: Build failed - dbgen not found"
    exit 1
fi

echo "  Build successful!"
echo ""
echo "========================================"
echo "dbgen installed at: ${DBGEN_DIR}/dbgen"
echo ""
echo "To generate TPC-H data:"
echo "  cd ${DBGEN_DIR}"
echo "  ./dbgen -vf -s 1    # SF1 (1GB)"
echo ""
echo "Or use the loaders:"
echo "  ./load_tpch_dbgen.sh 1 tpch1g"
echo "  ./load_tpch_postgres.sh 1 tpch1g"
