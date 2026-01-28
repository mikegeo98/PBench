#!/bin/bash
# Set up TPC-DS dsdgen tool
#
# This downloads and builds the TPC-DS data generator (dsdgen).
# Based on the popular tpcds-kit fork that works on Linux.
#
# Usage:
#   ./setup_tpcds_dsdgen.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPCDS_DIR="${SCRIPT_DIR}/tpcds-data"
DSDGEN_DIR="${TPCDS_DIR}/tpcds-dsdgen"

echo "TPC-DS dsdgen Setup"
echo "========================================"
echo "Target: ${DSDGEN_DIR}"
echo "========================================"

mkdir -p "${TPCDS_DIR}"

# Check if already installed
if [ -x "${DSDGEN_DIR}/tools/dsdgen" ]; then
    echo ""
    echo "dsdgen already installed at ${DSDGEN_DIR}/tools/dsdgen"
    echo "To rebuild, remove ${DSDGEN_DIR} and run again."
    exit 0
fi

# Clone tpcds-kit
echo ""
echo "Step 1: Downloading TPC-DS tools..."
if [ -d "${DSDGEN_DIR}" ]; then
    echo "  Directory exists, removing..."
    rm -rf "${DSDGEN_DIR}"
fi

git clone https://github.com/gregrahn/tpcds-kit.git "${DSDGEN_DIR}"
echo "  Downloaded to ${DSDGEN_DIR}"

# Build dsdgen
echo ""
echo "Step 2: Building dsdgen..."
cd "${DSDGEN_DIR}/tools"

# The makefile needs OS to be set
# Add -fcommon to fix multiple definition errors with modern GCC
# Only build dsdgen target (not qgen which requires yacc/bison)
make OS=LINUX CFLAGS="-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DLINUX -g -Wall -fcommon" dsdgen 2>&1 || true

if [ ! -x "dsdgen" ]; then
    echo "ERROR: Build failed - dsdgen not found"
    exit 1
fi

echo "  Build successful!"
echo ""
echo "========================================"
echo "dsdgen installed at: ${DSDGEN_DIR}/tools/dsdgen"
echo ""
echo "To generate TPC-DS data:"
echo "  cd ${DSDGEN_DIR}/tools"
echo "  ./dsdgen -scale 1 -dir /path/to/output"
echo ""
echo "Or use the loaders:"
echo "  ./load_tpcds_postgres.sh 1 tpcds1g"
echo "  ./load_tpcds_dbgen.sh 1 tpcds1g"
