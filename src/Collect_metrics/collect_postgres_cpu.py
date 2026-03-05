"""
Postgres CPU time collection using OS-level /proc/<pid>/stat.

Runs TPC-H SF20 on PostgreSQL and captures actual CPU time consumed by the
backend process (user + system time from /proc), not just EXPLAIN ANALYZE
wall-clock. Also collects EXPLAIN ANALYZE timing and buffer stats for
comparison.

Usage:
    python collect_postgres_cpu.py
    python collect_postgres_cpu.py --rounds 5
    python collect_postgres_cpu.py --database tpch20g
"""

import argparse
import json
import os
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

CLK_TCK = os.sysconf("SC_CLK_TCK")  # clock ticks per second (usually 100)
PG_BLOCK_SIZE = 8192

INPUT_FILE  = Path("./metrics_witho/input/TPCH-tpch20g-sql-input-standard.json")
OUTPUT_FILE = Path("./metrics_witho/output/TPCH-tpch20g-sql-metrics-postgres-cpu.json")


# ── /proc-based CPU measurement ────────────────────────────────────────────

def read_proc_cpu(pid):
    """
    Read user + system CPU time (in seconds) for a given PID from /proc.

    Fields 14 (utime) and 15 (stime) in /proc/<pid>/stat are in clock ticks.
    Returns (user_s, system_s) or None if unreadable.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        # Fields are 1-indexed in the man page; in the split array they're 0-indexed.
        # Field 14 = utime (index 13), Field 15 = stime (index 14)
        utime = int(fields[13]) / CLK_TCK
        stime = int(fields[14]) / CLK_TCK
        return utime, stime
    except (FileNotFoundError, PermissionError, IndexError):
        return None


def get_backend_pid(conn):
    """Get the PID of the PostgreSQL backend process for this connection."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        return cur.fetchone()[0]


# ── Query execution with dual CPU measurement ──────────────────────────────

def run_query_with_cpu(conn, sql, timeout_s=120):
    """
    Execute *sql* and return metrics from both /proc CPU and EXPLAIN ANALYZE.

    Returns dict with:
        proc_cpu_ms       — actual OS-level CPU (user+sys) delta
        proc_user_ms      — OS-level user CPU delta
        proc_sys_ms       — OS-level system CPU delta
        explain_exec_ms   — EXPLAIN ANALYZE Execution Time
        explain_plan_ms   — EXPLAIN ANALYZE Planning Time
        explain_total_ms  — exec + plan
        scanned_bytes     — from buffer stats (shared_hit + shared_read) * 8KB
        wall_ms           — client-side wall-clock
        ok                — True if successful
    """
    result = {
        "proc_cpu_ms": 0.0, "proc_user_ms": 0.0, "proc_sys_ms": 0.0,
        "explain_exec_ms": 0.0, "explain_plan_ms": 0.0, "explain_total_ms": 0.0,
        "scanned_bytes": 0, "wall_ms": 0.0, "ok": False,
    }

    try:
        pid = get_backend_pid(conn)

        # Read CPU before
        cpu_before = read_proc_cpu(pid)

        # Run via EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) to get both
        # timing and buffer stats in one shot
        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"

        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute(explain_sql)
            plan_result = cur.fetchone()[0]
        wall = time.time() - t0
        result["wall_ms"] = wall * 1000

        # Read CPU after
        cpu_after = read_proc_cpu(pid)

        # /proc CPU delta
        if cpu_before and cpu_after:
            du = cpu_after[0] - cpu_before[0]
            ds = cpu_after[1] - cpu_before[1]
            result["proc_user_ms"] = du * 1000
            result["proc_sys_ms"] = ds * 1000
            result["proc_cpu_ms"] = (du + ds) * 1000

        # EXPLAIN ANALYZE timing
        plan = plan_result[0] if plan_result else {}
        result["explain_exec_ms"] = plan.get("Execution Time", 0)
        result["explain_plan_ms"] = plan.get("Planning Time", 0)
        result["explain_total_ms"] = result["explain_exec_ms"] + result["explain_plan_ms"]

        # Buffer stats → scanned bytes
        def sum_buffers(node):
            hit = node.get("Shared Hit Blocks", 0)
            read = node.get("Shared Read Blocks", 0)
            for child in node.get("Plans", []):
                ch, cr = sum_buffers(child)
                hit += ch
                read += cr
            return hit, read

        root = plan.get("Plan", {})
        hit, read = sum_buffers(root)
        result["scanned_bytes"] = (hit + read) * PG_BLOCK_SIZE

        result["ok"] = True

    except Exception as e:
        print(f"    [PG] Error: {e}")
        # Rollback so the connection stays usable
        try:
            conn.rollback()
        except Exception:
            pass

    return result


# ── Experiment driver ───────────────────────────────────────────────────────

def run_experiment(args):
    conn_params = {
        "port": os.getenv("PG_PORT", "5432"),
        "user": os.getenv("PG_USER", os.getenv("USER", "postgres")),
        "database": args.database,
        "options": f"-c statement_timeout={args.timeout * 1000}",
    }
    pg_host = os.getenv("PG_HOST", "")
    if pg_host:
        conn_params["host"] = pg_host
    pg_password = os.getenv("PG_PASSWORD", "")
    if pg_password:
        conn_params["password"] = pg_password

    # Load queries
    with open(INPUT_FILE) as f:
        raw_queries = json.load(f)
    queries = []
    for entry in raw_queries:
        sql = entry["query"]
        if "@" in sql:
            sql = sql[:sql.rfind("@")]
        queries.append(sql.strip())

    print(f"Loaded {len(queries)} queries")
    print(f"Connecting to PostgreSQL: {pg_host or 'localhost'}:{conn_params['port']}/{args.database}")
    print(f"Rounds: {args.rounds}, Timeout: {args.timeout}s")
    print(f"CLK_TCK: {CLK_TCK}")
    print()

    # Resume
    results = []
    done = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            results = json.load(f)
        for r in results:
            done.add((r["query_idx"], r["round"]))
        print(f"Resumed {len(results)} existing records\n")

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True

    pid = get_backend_pid(conn)
    proc_ok = read_proc_cpu(pid) is not None
    print(f"Backend PID: {pid}")
    print(f"/proc/{pid}/stat readable: {proc_ok}")
    if not proc_ok:
        print("WARNING: Cannot read /proc — OS CPU capture will be unavailable.")
        print("         This is expected if PostgreSQL runs in Docker or on a remote host.")
    print()

    total = len(queries) * args.rounds
    completed = len(done)

    for qi, sql in enumerate(queries):
        for rd in range(args.rounds):
            if (qi, rd) in done:
                continue

            label = f"Q{qi+1:02d} R={rd+1}"
            print(f"[{completed+1}/{total}] {label} ...", end=" ", flush=True)

            metrics = run_query_with_cpu(conn, sql, args.timeout)
            results.append({
                "query_idx": qi,
                "query_label": f"Q{qi+1:02d}",
                "round": rd,
                **metrics,
            })

            if metrics["ok"]:
                print(f"proc_cpu={metrics['proc_cpu_ms']:.1f}ms "
                      f"explain={metrics['explain_total_ms']:.1f}ms "
                      f"scan={metrics['scanned_bytes']/1024/1024:.1f}MB")
            else:
                print("FAIL")

            with open(OUTPUT_FILE, "w") as f:
                json.dump(results, f, indent=2)
            completed += 1

    conn.close()
    print(f"\nDone — {len(results)} records saved to {OUTPUT_FILE}")


def main():
    p = argparse.ArgumentParser(
        description="Collect TPC-H metrics on PostgreSQL with OS-level CPU capture."
    )
    p.add_argument("--database", default="tpch20g", help="PostgreSQL database (default: tpch20g)")
    p.add_argument("--rounds", type=int, default=3, help="Repetitions per query (default: 3)")
    p.add_argument("--timeout", type=int, default=120, help="Per-query timeout in seconds (default: 120)")
    args = p.parse_args()

    print("PostgreSQL CPU Collection — TPC-H SF20")
    print("=" * 50)
    run_experiment(args)


if __name__ == "__main__":
    main()
