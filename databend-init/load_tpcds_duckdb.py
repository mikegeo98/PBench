#!/usr/bin/env python3
"""
Load TPC-DS data into DuckDB.

DuckDB can generate TPC-DS data natively using its tpcds extension,
so no external data files are needed.

Usage:
    python load_tpcds_duckdb.py [database_path] [scale_factor]

Examples:
    python load_tpcds_duckdb.py tpcds1g.duckdb 1      # SF1 (~1GB)
    python load_tpcds_duckdb.py tpcds100m.duckdb 0.1  # SF0.1 (~100MB)
"""

import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


# TPC-DS tables in dependency order
TPCDS_TABLES = [
    # Dimension tables
    "call_center",
    "catalog_page",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "time_dim",
    "warehouse",
    "web_page",
    "web_site",
    "customer",
    # Fact tables
    "inventory",
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
]

# Expected row counts for SF1 (approximate)
EXPECTED_SF1 = {
    "call_center": 6,
    "catalog_page": 11718,
    "customer_address": 50000,
    "customer_demographics": 1920800,
    "date_dim": 73049,
    "household_demographics": 7200,
    "income_band": 20,
    "item": 18000,
    "promotion": 300,
    "reason": 35,
    "ship_mode": 20,
    "store": 12,
    "time_dim": 86400,
    "warehouse": 5,
    "web_page": 60,
    "web_site": 30,
    "customer": 100000,
    "inventory": 11745000,
    "store_sales": 2880404,
    "store_returns": 287999,
    "catalog_sales": 1441548,
    "catalog_returns": 144067,
    "web_sales": 719384,
    "web_returns": 71763,
}


def main():
    # Parse arguments
    db_path = sys.argv[1] if len(sys.argv) > 1 else "tpcds1g.duckdb"
    scale_factor = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    print("TPC-DS DuckDB Data Loader")
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

    # Install and load TPC-DS extension
    print("Installing TPC-DS extension...")
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")

    # Generate TPC-DS data
    print(f"\nGenerating TPC-DS SF{scale_factor} data...")
    start = time.time()
    conn.execute(f"CALL dsdgen(sf={scale_factor})")
    elapsed = time.time() - start
    print(f"Data generation completed in {elapsed:.1f}s")

    # Verify row counts
    print("\nVerifying row counts...")

    total = 0
    for table in TPCDS_TABLES:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            expected = int(EXPECTED_SF1.get(table, 0) * scale_factor)
            # Allow 10% variance for estimated counts
            if expected > 0 and abs(count - expected) < expected * 0.1:
                status = "OK"
            elif expected > 0:
                status = f"(expected ~{expected:,})"
            else:
                status = ""
            print(f"  {table:25} {count:>12,} {status}")
            total += count
        except Exception as e:
            print(f"  {table:25} ERROR: {e}")

    print(f"\n  {'TOTAL':25} {total:>12,}")

    # Show database size
    conn.close()
    db_size = db_file.stat().st_size / (1024 * 1024)
    print(f"\nDatabase size: {db_size:.1f} MB")

    print("\n" + "=" * 50)
    print("Done!")
    print(f"\nTo use with collect.py:")
    print(f"  python collect.py tpcds --no-databend --duckdb --duckdb-path {db_path}")


if __name__ == "__main__":
    main()
