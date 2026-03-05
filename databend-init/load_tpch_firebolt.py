#!/usr/bin/env python3
"""
Load TPC-H data into Firebolt-Core.

This script expects pre-generated TPC-H .tbl files in:
  databend-init/tpch-data/sf<SCALE>/

Usage:
    python load_tpch_firebolt.py [scale_factor] [database] [host] [port]

Examples:
    python load_tpch_firebolt.py 1 tpch1g
    python load_tpch_firebolt.py 1 tpch1g localhost 3473
"""

from __future__ import annotations

import sys
import time
import re
from pathlib import Path

import requests


TABLES = ["region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]

CREATE_STATEMENTS = [
    "CREATE TABLE region (r_regionkey INTEGER NOT NULL,r_name CHAR(25) NOT NULL,r_comment VARCHAR(152));",
    "CREATE TABLE nation (n_nationkey INTEGER NOT NULL,n_name CHAR(25) NOT NULL,n_regionkey INTEGER NOT NULL,n_comment VARCHAR(152));",
    "CREATE TABLE supplier (s_suppkey INTEGER NOT NULL,s_name CHAR(25) NOT NULL,s_address VARCHAR(40) NOT NULL,s_nationkey INTEGER NOT NULL,s_phone CHAR(15) NOT NULL,s_acctbal DECIMAL(15,2) NOT NULL,s_comment VARCHAR(101) NOT NULL);",
    """CREATE TABLE part (
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL
);""",
    """CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL
);""",
    """CREATE TABLE customer (
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
);""",
    """CREATE TABLE orders (
    o_orderkey INTEGER NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL
);""",
    """CREATE TABLE lineitem (
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
    l_comment VARCHAR(44) NOT NULL
);""",
]

EXPECTED_SF1 = {
    "region": 5,
    "nation": 25,
    "supplier": 10000,
    "part": 200000,
    "partsupp": 800000,
    "customer": 150000,
    "orders": 1500000,
    "lineitem": 6001215,
}


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def run_sql(api_url: str, sql: str) -> str:
    response = requests.post(api_url, data=sql.encode("utf-8"), timeout=300)
    if response.status_code >= 400:
        die(f"ERROR running SQL: {response.status_code}\n{response.text.strip()}")
    return response.text.strip()


def count_rows(api_url: str, table: str) -> int:
    raw = run_sql(api_url, f"SELECT COUNT(*) FROM {table};")
    for line in raw.splitlines():
        cleaned = line.strip().replace(",", "")
        if re.fullmatch(r"\d+", cleaned):
            return int(cleaned)
    die(f"ERROR: unexpected count result for table {table}: {raw}")

def human_size(path: Path) -> str:
    size = path.stat().st_size
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}{unit}"
        value /= 1024.0
    return f"{size}B"


def main() -> None:
    scale = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    database = sys.argv[2] if len(sys.argv) > 2 else "tpch1g"
    host = sys.argv[3] if len(sys.argv) > 3 else "localhost"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 3473

    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "tpch-data" / f"sf{scale}"
    api_url = f"http://{host}:{port}/?output_format=psql&database={database}"

    print("TPC-H Firebolt-Core Data Loader")
    print("========================================")
    print(f"Scale Factor: {scale}")
    print(f"Database: {database}")
    print(f"SQL API: {host}:{port}")
    print(f"Data dir: {data_dir}")
    print("========================================")

    # Note: local file check removed — COPY loads from S3 (MinIO), not local files

    print("\nStep 1: Creating database...")
    run_sql(api_url, f"CREATE DATABASE IF NOT EXISTS {database};")

    print("\nStep 2: Creating TPC-H schema...")
    for statement in CREATE_STATEMENTS:
        run_sql(api_url, statement)
    print("  Schema created")

    print("\nStep 3: Loading data...")
    for table in TABLES:
        print(f"  {table}... ", end="", flush=True)
        start = time.time()
        run_sql(api_url, f"COPY {table} FROM 's3://tpch/sf{scale}/{table}.tbl' WITH(CREDENTIALS=(AWS_ACCESS_KEY_ID ='minioadmin',AWS_SECRET_ACCESS_KEY = 'minioadmin'),HEADER = FALSE, TYPE = csv, DELIMITER='|');")
        elapsed = time.time() - start
        count = count_rows(api_url, table)
        print(f"{count} rows in {elapsed:.3f}s")

    print("\nStep 4: Verifying row counts...")
    total = 0
    for table in TABLES:
        count = count_rows(api_url, table)
        expected = EXPECTED_SF1[table] * scale
        status = "OK" if count == expected else f"(expected {expected})"
        print(f"  {table + ':':12} {count:>12,} {status}")
        total += count

    print(f"\n  {'TOTAL:':12} {total:>12,}")

    print("\nStep 5: Creating indexes...")
    print("  Index creation skipped (unsupported)")

    print("\n========================================")
    print("Done!")


if __name__ == "__main__":
    main()
