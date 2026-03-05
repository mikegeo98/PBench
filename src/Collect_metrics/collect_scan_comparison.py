"""
Data-scanned comparison between Firebolt and Databend.

Two experiments:
  1. Full-table scans (SUM aggregate) on small/medium/large tables
  2. Selectivity sweep: filter queries with varying selectivity

Both backends report 'scanned_bytes' — this script compares what each
system reports for logically identical operations.

Usage:
    python collect_scan_comparison.py
    python collect_scan_comparison.py --rounds 5
    python collect_scan_comparison.py --no-databend   # Firebolt only
    python collect_scan_comparison.py --no-firebolt   # Databend only
"""

import argparse
import json
import os
import time
from pathlib import Path

import requests as http_requests
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ───────────────────────────────────────────────────────────

DATABASE = "tpch20g"

# Tables by size (SF20 approximate row counts)
# agg_expr forces a real scan (COUNT(*) can be answered from metadata).
TABLES = {
    "supplier": {
        "rows_approx": 200_000, "category": "small",
        "agg_expr": "SUM(s_acctbal)",
    },
    "orders": {
        "rows_approx": 30_000_000, "category": "medium",
        "agg_expr": "SUM(o_totalprice)",
    },
    "lineitem": {
        "rows_approx": 120_000_000, "category": "large",
        "agg_expr": "SUM(l_extendedprice)",
    },
}

# Selectivity sweep on lineitem.l_shipdate
# l_shipdate ranges from 1992-01-02 to 1998-12-01 (~2557 days)
# Each threshold selects approximately the given fraction of rows.
SELECTIVITY_LEVELS = [
    ("1%",  "l_shipdate <= DATE '1992-01-27'"),
    ("5%",  "l_shipdate <= DATE '1992-05-10'"),
    ("10%", "l_shipdate <= DATE '1992-10-17'"),
    ("25%", "l_shipdate <= DATE '1993-08-19'"),
    ("50%", "l_shipdate <= DATE '1995-06-17'"),
    ("75%", "l_shipdate <= DATE '1997-04-15'"),
    ("100%", "1=1"),
]

OUTPUT_FILE = Path("./metrics_witho/output/scan_comparison_firebolt_databend.json")

# ── Firebolt helpers ────────────────────────────────────────────────────────

def firebolt_query(host, port, database, sql, timeout_s=120):
    """Execute SQL on Firebolt, return (cpu_ms, scanned_bytes, duration_ms, wall_ms, ok)."""
    api_url = (
        f"http://{host}:{port}/"
        f"?database={database}&enable_subresult_cache=false"
    )
    stats_url = (
        f"http://{host}:{port}/"
        "?output_format=TabSeparatedWithNamesAndTypes"
    )
    try:
        t0 = time.time()
        resp = http_requests.post(api_url, data=sql.encode("utf-8"), timeout=timeout_s)
        wall = time.time() - t0
        if resp.status_code >= 400:
            print(f"    [Firebolt] HTTP {resp.status_code}: {resp.text[:200]}")
            return 0, 0, 0, wall * 1000, False

        query_id = resp.headers.get("Firebolt-Query-Id", "")
        if not query_id:
            return wall * 1000, 0, wall * 1000, wall * 1000, True

        stats_sql = (
            "SELECT cpu_usage_us, scanned_bytes, duration_us "
            "FROM information_schema.engine_query_history "
            f"WHERE query_id = '{query_id}' "
            "AND status = 'ENDED_SUCCESSFULLY';"
        )
        for _ in range(15):
            time.sleep(1.0)
            sr = http_requests.post(stats_url, data=stats_sql.encode("utf-8"), timeout=10)
            lines = sr.text.strip().split("\n")
            if len(lines) >= 3:
                vals = lines[2].split("\t")
                cpu_us = int(vals[0]) if vals[0] != "\\N" else 0
                scanned = int(vals[1]) if vals[1] != "\\N" else 0
                dur_us = int(vals[2]) if vals[2] != "\\N" else 0
                return cpu_us / 1000.0, scanned, dur_us / 1000.0, wall * 1000, True

        return wall * 1000, 0, wall * 1000, wall * 1000, True

    except Exception as e:
        print(f"    [Firebolt] Error: {e}")
        return 0, 0, 0, 0, False


# ── Databend helpers ────────────────────────────────────────────────────────

def databend_query(host, port, database, sql, prom_host, prom_port, timeout_s=120):
    """Execute SQL on Databend, return (cpu_ms, scanned_bytes, duration_ms, wall_ms, ok).

    Mirrors collect.py's approach: Prometheus counter deltas for both CPU and
    scanned_bytes, with 6 s sleeps before and after to align with scrape intervals.
    """
    api_url = f"http://{host}:{port}/v1/query/"
    try:
        # Wait for a fresh Prometheus scrape before reading baselines
        # (matches collect.py — scrape interval is 5 s)
        time.sleep(6)

        cpu_before = _prom_counter(prom_host, prom_port,
                                   "sum(databend_process_cpu_seconds_total_total)")
        scan_before = _prom_counter(prom_host, prom_port,
                                    'sum(databend_query_scan_bytes_total{kind="Query"})')

        t0 = time.time()
        resp = http_requests.post(
            api_url,
            json={
                "sql": sql,
                "session": {"database": database},
            },
            headers={"Content-Type": "application/json"},
            auth=("root", ""),
            timeout=timeout_s,
        )
        wall = time.time() - t0

        if resp.status_code >= 400:
            print(f"    [Databend] HTTP {resp.status_code}: {resp.text[:200]}")
            return 0, 0, 0, wall * 1000, False

        # Drain paginated results
        body = resp.json()

        # Check for query error in response body
        err = body.get("error")
        if err and isinstance(err, dict) and err.get("message"):
            print(f"    [Databend] Query error: {err['message'][:200]}")
            return 0, 0, 0, wall * 1000, False

        next_uri = body.get("next_uri")
        while next_uri:
            r2 = http_requests.get(
                f"http://{host}:{port}{next_uri}",
                auth=("root", ""),
                timeout=timeout_s,
            )
            body = r2.json()
            next_uri = body.get("next_uri")

        duration_ms = wall * 1000

        # Wait for Prometheus to scrape the new counter values
        time.sleep(6)

        cpu_after = _prom_counter(prom_host, prom_port,
                                  "sum(databend_process_cpu_seconds_total_total)")
        scan_after = _prom_counter(prom_host, prom_port,
                                   'sum(databend_query_scan_bytes_total{kind="Query"})')

        cpu_ms = (cpu_after - cpu_before) * 1000  # seconds → ms
        scanned = scan_after - scan_before

        return cpu_ms, int(scanned), duration_ms, wall * 1000, True

    except Exception as e:
        print(f"    [Databend] Error: {e}")
        return 0, 0, 0, 0, False


def _prom_counter(host, port, query):
    """Read a single Prometheus instant-query value."""
    try:
        url = f"http://{host}:{port}/api/v1/query"
        r = http_requests.get(url, params={"query": query}, timeout=5)
        data = r.json()
        results = data.get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception:
        pass
    return 0.0


# ── Experiment driver ───────────────────────────────────────────────────────

def run_experiment(args):
    fb_host = os.getenv("FIREBOLT_HOST", "localhost")
    fb_port = os.getenv("FIREBOLT_PORT", "3473")
    db_host = os.getenv("HOST", "localhost")
    db_port = os.getenv("DATABEND_PORT", "8000")
    prom_host = db_host
    prom_port = os.getenv("PROMETHEUS_PORT", "9091")

    results = []
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            results = json.load(f)
        print(f"Resumed {len(results)} existing records")

    done = set()
    for r in results:
        done.add((r["experiment"], r["table"], r["selectivity"], r["backend"], r["round"]))

    # ── Experiment 1: Full table scans ──
    # Use an aggregate (SUM) instead of SELECT * to force a real scan
    # without transferring millions of rows (OOM on lineitem).
    # COUNT(*) is optimised away by Databend (metadata shortcut).
    for table, info in TABLES.items():
        agg = info["agg_expr"]
        sql = f"SELECT {agg} FROM {table}"
        for rd in range(args.rounds):
            for backend in ["firebolt", "databend"]:
                if (backend == "firebolt" and args.no_firebolt) or \
                   (backend == "databend" and args.no_databend):
                    continue
                key = ("full_scan", table, "100%", backend, rd)
                if key in done:
                    continue

                print(f"  Full scan: {table} ({info['category']}) "
                      f"| {backend} | round {rd+1}/{args.rounds} ...", end=" ", flush=True)

                if backend == "firebolt":
                    cpu, scan, dur, wall, ok = firebolt_query(
                        fb_host, fb_port, DATABASE, sql, args.timeout)
                else:
                    cpu, scan, dur, wall, ok = databend_query(
                        db_host, db_port, DATABASE, sql,
                        prom_host, prom_port, args.timeout)

                results.append({
                    "experiment": "full_scan",
                    "table": table,
                    "category": info["category"],
                    "selectivity": "100%",
                    "backend": backend,
                    "round": rd,
                    "sql": sql,
                    "cpu_ms": cpu,
                    "scanned_bytes": scan,
                    "duration_ms": dur,
                    "wall_ms": wall,
                    "ok": ok,
                })
                print("ok" if ok else "FAIL")
                _save(results)

    # ── Experiment 2: Selectivity sweep on lineitem ──
    li_agg = TABLES["lineitem"]["agg_expr"]
    for sel_label, predicate in SELECTIVITY_LEVELS:
        sql = f"SELECT {li_agg} FROM lineitem WHERE {predicate}"
        for rd in range(args.rounds):
            for backend in ["firebolt", "databend"]:
                if (backend == "firebolt" and args.no_firebolt) or \
                   (backend == "databend" and args.no_databend):
                    continue
                key = ("selectivity", "lineitem", sel_label, backend, rd)
                if key in done:
                    continue

                print(f"  Selectivity {sel_label}: lineitem "
                      f"| {backend} | round {rd+1}/{args.rounds} ...", end=" ", flush=True)

                if backend == "firebolt":
                    cpu, scan, dur, wall, ok = firebolt_query(
                        fb_host, fb_port, DATABASE, sql, args.timeout)
                else:
                    cpu, scan, dur, wall, ok = databend_query(
                        db_host, db_port, DATABASE, sql,
                        prom_host, prom_port, args.timeout)

                results.append({
                    "experiment": "selectivity",
                    "table": "lineitem",
                    "category": "large",
                    "selectivity": sel_label,
                    "backend": backend,
                    "round": rd,
                    "sql": sql,
                    "cpu_ms": cpu,
                    "scanned_bytes": scan,
                    "duration_ms": dur,
                    "wall_ms": wall,
                    "ok": ok,
                })
                print("ok" if ok else "FAIL")
                _save(results)

    print(f"\nDone — {len(results)} records saved to {OUTPUT_FILE}")


def _save(results):
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)


def main():
    p = argparse.ArgumentParser(description="Compare data-scanned telemetry between Firebolt and Databend.")
    p.add_argument("--rounds", type=int, default=3, help="Repetitions per experiment (default: 3)")
    p.add_argument("--timeout", type=int, default=300, help="Per-query timeout in seconds (default: 300)")
    p.add_argument("--no-firebolt", action="store_true", help="Skip Firebolt")
    p.add_argument("--no-databend", action="store_true", help="Skip Databend")
    args = p.parse_args()

    print("Data-Scanned Comparison: Firebolt vs Databend")
    print(f"Database: {DATABASE}")
    print(f"Tables: {list(TABLES.keys())}")
    print(f"Selectivity levels: {[s[0] for s in SELECTIVITY_LEVELS]}")
    print(f"Rounds: {args.rounds}")
    print()

    run_experiment(args)


if __name__ == "__main__":
    main()
