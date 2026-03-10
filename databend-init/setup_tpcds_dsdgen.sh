#!/bin/bash
# Build TPC-DS dsdgen tool
#
# Downloads TPC-DS tools from the official repository and compiles dsdgen.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DSDGEN_DIR="${SCRIPT_DIR}/tpcds-data/tpcds-dsdgen"

echo "Setting up TPC-DS dsdgen..."
echo "========================================"

# Install build dependencies
echo "Installing build dependencies..."
sudo apt-get update -qq && sudo apt-get install -y -qq gcc make flex bison

# Clone TPC-DS tools if not present
if [ ! -d "${DSDGEN_DIR}" ]; then
    echo "Cloning TPC-DS tools..."
    mkdir -p "${SCRIPT_DIR}/tpcds-data"
    git clone https://github.com/gregrahn/tpcds-kit.git "${DSDGEN_DIR}"
else
    echo "TPC-DS tools already cloned at ${DSDGEN_DIR}"
fi

# Build dsdgen
echo "Building dsdgen..."
cd "${DSDGEN_DIR}/tools"
make OS=LINUX clean
make OS=LINUX

if [ -x "${DSDGEN_DIR}/tools/dsdgen" ]; then
    echo ""
    echo "========================================"
    echo "dsdgen built successfully at:"
    echo "  ${DSDGEN_DIR}/tools/dsdgen"
    echo ""
    echo "Now run: ./load_tpcds_dbgen.sh <scale_factor> <database>"
else
    echo "ERROR: Build failed — dsdgen not found"
    exit 1
fi
