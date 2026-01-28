#!/usr/bin/env python3
"""
Load TPC-H data into DuckDB.

DuckDB can generate TPC-H data natively using its tpch extension,
so no external data files are needed.

Usage:
    python load_tpch_duckdb.py [database_path] [scale_factor]

Examples:
    python load_tpch_duckdb.py tpch1g.duckdb 1      # SF1 (~1GB)
    python load_tpch_duckdb.py tpch100m.duckdb 0.1  # SF0.1 (~100MB)
"""

import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


def main():
    # Parse arguments
    db_path = sys.argv[1] if len(sys.argv) > 1 else "tpch1g.duckdb"
    scale_factor = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    print("TPC-H DuckDB Data Loader")
    print("=" * 50)
    print(f"Database: {db_path}")
    print(f"Scale Factor: {scale_factor} (~{scale_factor}GB)")
    print("=" * 50)

    # Remove existing database if it exists
    db_file = Path(db_path)
    if db_file.exists():
        print(f"\nRemoving existing database: {db_path}")
        db_file.unlink()

    # Connect to DuckDB
    print("\nConnecting to DuckDB...")
    conn = duckdb.connect(db_path)

    # Install and load TPC-H extension
    print("Installing TPC-H extension...")
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")

    # Generate TPC-H data
    print(f"\nGenerating TPC-H SF{scale_factor} data...")
    start = time.time()
    conn.execute(f"CALL dbgen(sf={scale_factor})")
    elapsed = time.time() - start
    print(f"Data generation completed in {elapsed:.1f}s")

    # Verify row counts
    print("\nVerifying row counts...")
    tables = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

    # Expected counts for SF1
    expected_sf1 = {
        "region": 5,
        "nation": 25,
        "supplier": 10000,
        "customer": 150000,
        "part": 200000,
        "partsupp": 800000,
        "orders": 1500000,
        "lineitem": 6001215
    }

    total = 0
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        expected = int(expected_sf1.get(table, 0) * scale_factor)
        status = "OK" if abs(count - expected) < expected * 0.01 else f"(expected ~{expected:,})"
        print(f"  {table:12} {count:>12,} {status}")
        total += count

    print(f"\n  {'TOTAL':12} {total:>12,}")

    # Show database size
    conn.close()
    db_size = db_file.stat().st_size / (1024 * 1024)
    print(f"\nDatabase size: {db_size:.1f} MB")

    print("\n" + "=" * 50)
    print("Done!")
    print(f"\nTo use with collect.py:")
    print(f"  python collect.py tpch --no-databend --duckdb --duckdb-path {db_path}")


if __name__ == "__main__":
    main()
