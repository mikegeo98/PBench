"""
Concurrency telemetry collection for Firebolt.

Runs each TPC-H query at varying concurrency levels (default: 1, 2, 4, 8),
collects per-execution telemetry from engine_query_history, and writes a
single JSON results file that the analysis notebook consumes.

Usage:
    python collect_concurrency.py                          # defaults
    python collect_concurrency.py --concurrency 1 2 4 8 16
    python collect_concurrency.py --rounds 5 --timeout 180
    python collect_concurrency.py --benchmark tpch20       # default
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import subprocess

import requests as http_requests
from dotenv import load_dotenv

load_dotenv()

# ── Benchmark definitions (same keys as collect.py) ─────────────────────────

BENCHMARKS = {
    "tpch20": {
        "input_firebolt": "./metrics_witho/input/TPCH-tpch20g-sql-input-standard.json",
        "database": "tpch20g",
        "output": "./metrics_witho/output/concurrency_telemetry_firebolt.json",
        "description": "TPC-H SF20 (22 queries)",
    },
    "tpch": {
        "input_firebolt": "./metrics_witho/input/TPCH-tpch1g-sql-input-standard.json",
        "database": "tpch1g",
        "output": "./metrics_witho/output/concurrency_telemetry_firebolt_sf1.json",
        "description": "TPC-H SF1 (22 queries)",
    },
    "tpch10g": {
        "input_firebolt": "./metrics_witho/input/TPCH-tpch10g-sql-input.json",
        "database": "tpch10g",
        "output": "./metrics_witho/output/concurrency_telemetry_firebolt_sf10.json",
        "description": "TPC-H SF10 (22 queries)",
    },
}

# ── Firebolt helpers ────────────────────────────────────────────────────────


def run_query_and_collect(api_url, stats_url, sql, timeout_s, label=""):
    """
    Execute *sql* on Firebolt and return telemetry from engine_query_history.

    Returns dict with keys:
        cpu_ms, scanned_bytes, duration_ms, wall_ms, query_id, ok
    """
    result = {
        "cpu_ms": 0.0,
        "scanned_bytes": 0,
        "duration_ms": 0.0,
        "wall_ms": 0.0,
        "query_id": "",
        "ok": False,
    }
    try:
        t0 = time.time()
        resp = http_requests.post(
            api_url, data=sql.encode("utf-8"), timeout=timeout_s
        )
        wall = time.time() - t0
        result["wall_ms"] = wall * 1000

        if resp.status_code >= 400:
            print(f"  [{label}] HTTP {resp.status_code}: {resp.text[:200]}")
            return result

        query_id = resp.headers.get("Firebolt-Query-Id", "")
        result["query_id"] = query_id

        if not query_id:
            result["duration_ms"] = wall * 1000
            result["cpu_ms"] = wall * 1000
            result["ok"] = True
            return result

        # Poll engine_query_history (async write delay ~2-3 s)
        stats_sql = (
            "SELECT cpu_usage_us, scanned_bytes, duration_us "
            "FROM information_schema.engine_query_history "
            f"WHERE query_id = '{query_id}' "
            "AND status = 'ENDED_SUCCESSFULLY';"
        )
        for _ in range(15):
            time.sleep(1.0)
            sr = http_requests.post(
                stats_url, data=stats_sql.encode("utf-8"), timeout=10
            )
            lines = sr.text.strip().split("\n")
            if len(lines) >= 3:
                vals = lines[2].split("\t")
                cpu_us = int(vals[0]) if vals[0] != "\\N" else 0
                scanned = int(vals[1]) if vals[1] != "\\N" else 0
                dur_us = int(vals[2]) if vals[2] != "\\N" else 0
                result["cpu_ms"] = cpu_us / 1000.0
                result["scanned_bytes"] = scanned
                result["duration_ms"] = dur_us / 1000.0
                result["ok"] = True
                return result

        # Fallback: wall-clock
        result["cpu_ms"] = wall * 1000
        result["duration_ms"] = wall * 1000
        result["ok"] = True

    except Exception as e:
        print(f"  [{label}] Error: {e}")

    return result


# ── Health check ────────────────────────────────────────────────────────────

MAX_HEALTH_WAIT = 300  # seconds to wait for Firebolt before aborting

# The compose.yaml lives in PBench/databend-init/firebolt-core/ and creates
# a container named ${COMPOSE_PROJECT_NAME}-node-0 (default: firebolt-node-0).
FIREBOLT_COMPOSE_DIR = os.getenv(
    "FIREBOLT_COMPOSE_DIR",
    str(Path(__file__).resolve().parent.parent.parent / "databend-init" / "firebolt-core"),
)
FIREBOLT_COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "firebolt")


def restart_firebolt():
    """Restart the Firebolt-Core cluster via docker compose."""
    compose_dir = Path(FIREBOLT_COMPOSE_DIR)
    compose_file = compose_dir / "compose.yaml"

    if compose_file.exists():
        print(f"\n  Restarting Firebolt via docker compose in {compose_dir}...", flush=True)
        try:
            env = {**os.environ, "PWD": str(compose_dir)}
            subprocess.run(
                ["docker", "compose", "-p", FIREBOLT_COMPOSE_PROJECT,
                 "up", "-d", "--wait", "core"],
                cwd=str(compose_dir), env=env,
                check=True, capture_output=True, timeout=120,
            )
            print("  docker compose up -d --wait core succeeded.")
            return
        except subprocess.CalledProcessError as e:
            print(f"  docker compose up failed: {e.stderr.decode().strip()}")
        except FileNotFoundError:
            print("  docker not found on PATH — cannot restart automatically.")
    else:
        print(f"\n  compose.yaml not found at {compose_file}")
        print("  Set FIREBOLT_COMPOSE_DIR to the directory containing compose.yaml.")


def wait_for_firebolt(stats_url, max_wait=MAX_HEALTH_WAIT):
    """
    Block until Firebolt responds to a SELECT 1, or abort after *max_wait* s.

    If unreachable, restarts the Docker container and waits for it to come up.
    Returns True if healthy, False if timed out.
    """
    # Quick check — already up?
    try:
        r = http_requests.post(stats_url, data=b"SELECT 1", timeout=5)
        if r.status_code < 400:
            return True
    except Exception:
        pass

    # Not reachable — restart and wait
    restart_firebolt()

    deadline = time.time() + max_wait
    attempt = 0
    while time.time() < deadline:
        try:
            r = http_requests.post(
                stats_url, data=b"SELECT 1", timeout=5
            )
            if r.status_code < 400:
                print(f"  Firebolt is back (after {attempt} retries)")
                return True
        except http_requests.ConnectionError:
            pass
        except Exception:
            pass
        attempt += 1
        if attempt == 1:
            print(
                f"  Waiting up to {max_wait}s for Firebolt to come up...",
                end="",
                flush=True,
            )
        elif attempt % 10 == 0:
            remaining = int(deadline - time.time())
            print(f" ({remaining}s left)", end="", flush=True)
        time.sleep(3)

    print(f"\n  ABORT: Firebolt did not recover within {max_wait}s")
    return False


# ── Experiment driver ───────────────────────────────────────────────────────


def run_concurrency_experiment(
    queries, concurrency_levels, rounds, output_path, api_url, stats_url, timeout_s
):
    """
    Run the full concurrency sweep.

    For each (query, concurrency_level, round):
      - submit C copies in parallel via ThreadPoolExecutor
      - collect telemetry for each copy
      - save incrementally to output_path (resume-safe)
    """
    results = []
    done = set()  # (query_idx, concurrency, round)

    if output_path.exists():
        with open(output_path) as f:
            results = json.load(f)
        for r in results:
            done.add((r["query_idx"], r["concurrency"], r["round"]))
        print(
            f"Resumed {len(results)} existing records "
            f"({len(done)} query/concurrency/round combos done)"
        )

    total = len(queries) * len(concurrency_levels) * rounds
    completed = len(done)
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3  # full-zero combos before health check

    for qi, sql in enumerate(queries):
        for C in concurrency_levels:
            for rd in range(rounds):
                if (qi, C, rd) in done:
                    continue

                # If we've had several total-failure combos, check if Firebolt
                # is still alive before wasting more attempts.
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n  {consecutive_failures} consecutive failures — "
                          "checking Firebolt health...")
                    if not wait_for_firebolt(stats_url):
                        print("Saving partial results and exiting.")
                        with open(output_path, "w") as f:
                            json.dump(results, f, indent=2)
                        return results
                    consecutive_failures = 0

                label = f"Q{qi+1:02d} C={C} R={rd+1}"
                print(
                    f"[{completed+1}/{total}] {label} ...",
                    end=" ",
                    flush=True,
                )

                # Submit C copies in parallel
                with ThreadPoolExecutor(max_workers=C) as pool:
                    futures = []
                    for copy_idx in range(C):
                        tag = f"{label} copy={copy_idx}"
                        futures.append(
                            pool.submit(
                                run_query_and_collect,
                                api_url,
                                stats_url,
                                sql,
                                timeout_s,
                                tag,
                            )
                        )
                    copy_results = [f.result() for f in futures]

                ok_count = sum(1 for cr in copy_results if cr["ok"])

                # Only record results when at least one copy succeeded
                if ok_count > 0:
                    for copy_idx, cr in enumerate(copy_results):
                        results.append(
                            {
                                "query_idx": qi,
                                "concurrency": C,
                                "round": rd,
                                "copy": copy_idx,
                                **cr,
                            }
                        )
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                print(f"{ok_count}/{C} ok")

                # Save after every combo
                with open(output_path, "w") as f:
                    json.dump(results, f, indent=2)

                completed += 1

    print(f"\nDone — {len(results)} total records saved to {output_path}")
    return results


# ── CLI ─────────────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(
        description="Collect per-query telemetry at varying concurrency levels."
    )
    p.add_argument(
        "benchmark",
        nargs="?",
        default="tpch20",
        choices=BENCHMARKS.keys(),
        help="Benchmark to run (default: tpch20)",
    )
    p.add_argument(
        "--concurrency",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8],
        help="Concurrency levels to test (default: 1 2 4 8)",
    )
    p.add_argument(
        "--rounds",
        type=int,
        default=3,
        help="Repetitions per concurrency level (default: 3)",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Per-query timeout in seconds (default: 120)",
    )
    p.add_argument(
        "--output",
        type=str,
        default=None,
        help="Override output file path",
    )
    return p.parse_args()


def main():
    args = parse_args()
    bench = BENCHMARKS[args.benchmark]

    firebolt_host = os.getenv("FIREBOLT_HOST", "localhost")
    firebolt_port = os.getenv("FIREBOLT_PORT", "3473")
    database = bench["database"]

    api_url = (
        f"http://{firebolt_host}:{firebolt_port}/"
        f"?database={database}&enable_subresult_cache=false"
    )
    stats_url = (
        f"http://{firebolt_host}:{firebolt_port}/"
        "?output_format=TabSeparatedWithNamesAndTypes"
    )

    input_path = Path(bench["input_firebolt"])
    output_path = Path(args.output) if args.output else Path(bench["output"])

    print(f"Benchmark:   {args.benchmark} — {bench['description']}")
    print(f"Firebolt:    http://{firebolt_host}:{firebolt_port}")
    print(f"Database:    {database}")
    print(f"Input:       {input_path}")
    print(f"Output:      {output_path}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Rounds:      {args.rounds}")
    print(f"Timeout:     {args.timeout}s")
    print()

    # Load queries
    with open(input_path) as f:
        raw_queries = json.load(f)

    queries = []
    for entry in raw_queries:
        sql = entry["query"]
        if "@" in sql:
            sql = sql[: sql.rfind("@")]
        queries.append(sql.strip())

    print(f"Loaded {len(queries)} queries\n")

    # Wait for Firebolt to be ready before starting
    print("Checking Firebolt connectivity...", end=" ", flush=True)
    if not wait_for_firebolt(stats_url):
        print("Firebolt is not reachable. Exiting.")
        return
    print("OK\n")

    # Run experiment
    run_concurrency_experiment(
        queries=queries,
        concurrency_levels=sorted(args.concurrency),
        rounds=args.rounds,
        output_path=output_path,
        api_url=api_url,
        stats_url=stats_url,
        timeout_s=args.timeout,
    )


if __name__ == "__main__":
    main()
