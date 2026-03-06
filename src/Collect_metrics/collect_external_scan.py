"""
Local Parquet vs S3 Parquet scan comparison on Databend.

Both modes scan the same Parquet files — the only difference is where
they're read from:
  - local:    Parquet on local filesystem via a Databend stage (fs://)
  - external: Parquet on S3/MinIO via direct S3 URI

This isolates the storage-layer overhead (local disk vs object storage)
with the same file format, same engine, same queries.

Experiments:
  1. Full-table scans (SUM aggregate) on small/medium/large tables
  2. Selectivity sweep on store_sales (largest fact table)

Prerequisites:
  1. Convert .dat -> .parquet:
       python convert_tpcds_to_parquet.py --scale 20 --upload
  2. Mount parquet dir in Databend container (for local mode):
       Add volume: ./databend-init/tpcds-data/sf20/parquet:/parquet_data
  3. Create stage in Databend (once):
       CREATE OR REPLACE STAGE parquet_local URL='fs:///parquet_data/' FILE_FORMAT=(type=PARQUET);

Usage:
    python collect_external_scan.py
    python collect_external_scan.py --rounds 3 --timeout 300
    python collect_external_scan.py --no-local    # S3 only
    python collect_external_scan.py --no-external # local only
    python collect_external_scan.py --local-dir /path/to/parquet  # direct fs path (no stage)
"""

import argparse
import json
import os
import time
from pathlib import Path

import requests as http_requests
from dotenv import load_dotenv

load_dotenv()

# -- Configuration -----------------------------------------------------------

DATABASE = "tpcds20g"
S3_BUCKET = "tpcds"
LOCAL_STAGE = "parquet_local"  # Databend stage name for local parquet

# Tables by size category (TPC-DS SF20 approximate row counts)
TABLES = {
    "income_band": {
        "rows_approx": 20, "category": "tiny",
        "agg_expr": "SUM(ib_upper_bound)",
    },
    "ship_mode": {
        "rows_approx": 20, "category": "tiny",
        "agg_expr": "COUNT(sm_carrier)",
    },
    "customer": {
        "rows_approx": 2_000_000, "category": "small",
        "agg_expr": "SUM(c_birth_year)",
    },
    "catalog_returns": {
        "rows_approx": 2_880_000, "category": "medium",
        "agg_expr": "SUM(cr_return_amount)",
    },
    "store_sales": {
        "rows_approx": 57_000_000, "category": "large",
        "agg_expr": "SUM(ss_net_paid)",
    },
}

# Selectivity sweep on store_sales.ss_sold_date_sk
# ss_sold_date_sk ranges roughly from 2450816 to 2452642 (~1826 values for SF20)
SELECTIVITY_LEVELS = [
    ("1%",   "ss_sold_date_sk <= 2450834"),
    ("5%",   "ss_sold_date_sk <= 2450907"),
    ("10%",  "ss_sold_date_sk <= 2450999"),
    ("25%",  "ss_sold_date_sk <= 2451273"),
    ("50%",  "ss_sold_date_sk <= 2451729"),
    ("75%",  "ss_sold_date_sk <= 2452186"),
    ("100%", "1=1"),
]

OUTPUT_FILE = Path("./metrics_witho/output/external_scan_comparison.json")


# -- Helpers ------------------------------------------------------------------

def s3_connection_clause(endpoint, access_key, secret_key):
    return (
        f"(CONNECTION => ("
        f"ENDPOINT_URL = '{endpoint}', "
        f"ACCESS_KEY_ID = '{access_key}', "
        f"SECRET_ACCESS_KEY = '{secret_key}'"
        f"))"
    )


def databend_query(host, port, sql, database=None, timeout_s=120):
    """Execute SQL on Databend, return (wall_ms, ok, stats_dict)."""
    api_url = f"http://{host}:{port}/v1/query/"
    body = {"sql": sql}
    if database:
        body["session"] = {"database": database}

    try:
        t0 = time.time()
        resp = http_requests.post(
            api_url,
            json=body,
            headers={"Content-Type": "application/json"},
            auth=("root", ""),
            timeout=timeout_s,
        )
        wall = time.time() - t0

        if resp.status_code >= 400:
            print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
            return wall * 1000, False, None

        result = resp.json()

        err = result.get("error")
        if err and isinstance(err, dict) and err.get("message"):
            print(f"    Query error: {err['message'][:200]}")
            return wall * 1000, False, None

        # Drain paginated results
        next_uri = result.get("next_uri")
        while next_uri:
            r2 = http_requests.get(
                f"http://{host}:{port}{next_uri}",
                auth=("root", ""),
                timeout=timeout_s,
            )
            result = r2.json()
            next_uri = result.get("next_uri")

        # Extract scan stats from response
        stats = result.get("stats", {})
        scan_progress = stats.get("scan_progress", {})
        scanned_bytes = scan_progress.get("bytes", 0)
        scanned_rows = scan_progress.get("rows", 0)

        return wall * 1000, True, {
            "scanned_bytes": scanned_bytes,
            "scanned_rows": scanned_rows,
        }

    except Exception as e:
        print(f"    Error: {e}")
        return 0, False, None


def build_sql(mode, table, agg_expr, conn, bucket, stage, where_clause=None):
    """Build the SELECT SQL for the given mode (local or external)."""
    if mode == "local":
        source = f"@{stage}/{table}.parquet"
    else:
        source = f"'s3://{bucket}/{table}.parquet' {conn}"

    sql = f"SELECT {agg_expr} FROM {source}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    return sql


def run_experiment(args):
    db_host = os.getenv("HOST", "localhost")
    db_port = os.getenv("DATABEND_PORT", "8000")
    conn = s3_connection_clause(args.s3_endpoint, args.s3_access_key, args.s3_secret_key)
    bucket = args.bucket
    stage = args.stage

    # Ensure the local stage exists
    if not args.no_local:
        print(f"Ensuring local stage '{stage}' exists...")
        setup_sql = (
            f"CREATE OR REPLACE STAGE {stage} "
            f"URL='fs://{args.parquet_mount}/' "
            f"FILE_FORMAT=(type=PARQUET)"
        )
        wall_ms, ok, _ = databend_query(db_host, db_port, setup_sql, database=DATABASE)
        if ok:
            print(f"  Stage '{stage}' ready (mount: {args.parquet_mount})")
        else:
            print(f"  WARNING: Could not create stage. Local scans may fail.")

    results = []
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            results = json.load(f)
        print(f"Resumed {len(results)} existing records")

    done = set()
    for r in results:
        done.add((r["experiment"], r["table"], r["selectivity"], r["mode"], r["round"]))

    # -- Experiment 1: Full table scans --
    for table, info in TABLES.items():
        agg = info["agg_expr"]
        for rd in range(args.rounds):
            for mode in ["local", "external"]:
                if (mode == "local" and args.no_local) or \
                   (mode == "external" and args.no_external):
                    continue
                key = ("full_scan", table, "100%", mode, rd)
                if key in done:
                    continue

                sql = build_sql(mode, table, agg, conn, bucket, stage)

                print(f"  Full scan: {table} ({info['category']}) "
                      f"| {mode} | round {rd+1}/{args.rounds} ...", end=" ", flush=True)

                wall_ms, ok, stats = databend_query(
                    db_host, db_port, sql,
                    database=DATABASE,
                    timeout_s=args.timeout,
                )

                results.append({
                    "experiment": "full_scan",
                    "table": table,
                    "category": info["category"],
                    "selectivity": "100%",
                    "mode": mode,
                    "round": rd,
                    "sql": f"SELECT {agg} FROM {'@stage' if mode == 'local' else 's3://'}.../{table}.parquet",
                    "wall_ms": wall_ms,
                    "scanned_bytes": stats["scanned_bytes"] if stats else 0,
                    "scanned_rows": stats["scanned_rows"] if stats else 0,
                    "ok": ok,
                })
                print(f"{'ok' if ok else 'FAIL'} ({wall_ms:.0f}ms)")
                _save(results)

    # -- Experiment 2: Selectivity sweep on store_sales --
    agg = TABLES["store_sales"]["agg_expr"]
    for sel_label, predicate in SELECTIVITY_LEVELS:
        for rd in range(args.rounds):
            for mode in ["local", "external"]:
                if (mode == "local" and args.no_local) or \
                   (mode == "external" and args.no_external):
                    continue
                key = ("selectivity", "store_sales", sel_label, mode, rd)
                if key in done:
                    continue

                sql = build_sql(mode, "store_sales", agg, conn, bucket, stage, predicate)

                print(f"  Selectivity {sel_label}: store_sales "
                      f"| {mode} | round {rd+1}/{args.rounds} ...", end=" ", flush=True)

                wall_ms, ok, stats = databend_query(
                    db_host, db_port, sql,
                    database=DATABASE,
                    timeout_s=args.timeout,
                )

                results.append({
                    "experiment": "selectivity",
                    "table": "store_sales",
                    "category": "large",
                    "selectivity": sel_label,
                    "mode": mode,
                    "round": rd,
                    "sql": f"SELECT {agg} FROM ...store_sales.parquet WHERE {predicate}",
                    "wall_ms": wall_ms,
                    "scanned_bytes": stats["scanned_bytes"] if stats else 0,
                    "scanned_rows": stats["scanned_rows"] if stats else 0,
                    "ok": ok,
                })
                print(f"{'ok' if ok else 'FAIL'} ({wall_ms:.0f}ms)")
                _save(results)

    print(f"\nDone -- {len(results)} records saved to {OUTPUT_FILE}")


def _save(results):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)


def main():
    p = argparse.ArgumentParser(
        description="Compare local Parquet vs S3 Parquet scan on Databend."
    )
    p.add_argument("--rounds", type=int, default=3, help="Repetitions per experiment (default: 3)")
    p.add_argument("--timeout", type=int, default=300, help="Per-query timeout in seconds")
    p.add_argument("--no-local", action="store_true", help="Skip local Parquet scans")
    p.add_argument("--no-external", action="store_true", help="Skip S3 Parquet scans")
    p.add_argument("--bucket", default=S3_BUCKET, help="S3/MinIO bucket (default: tpcds)")
    p.add_argument("--s3-endpoint", default="http://localhost:9000", help="MinIO/S3 endpoint")
    p.add_argument("--s3-access-key", default="minioadmin", help="S3 access key")
    p.add_argument("--s3-secret-key", default="minioadmin", help="S3 secret key")
    p.add_argument("--stage", default=LOCAL_STAGE, help="Databend stage name for local parquet")
    p.add_argument("--parquet-mount", default="/parquet_data",
                    help="Path inside Databend container where parquet files are mounted")
    args = p.parse_args()

    print("Local Parquet vs S3 Parquet Scan Comparison (Databend)")
    print(f"Database: {DATABASE}")
    print(f"Local stage: @{args.stage} -> {args.parquet_mount}")
    print(f"S3 bucket: s3://{args.bucket}/")
    print(f"Tables: {list(TABLES.keys())}")
    print(f"Selectivity levels: {[s[0] for s in SELECTIVITY_LEVELS]}")
    print(f"Rounds: {args.rounds}")
    print()

    run_experiment(args)


if __name__ == "__main__":
    main()
