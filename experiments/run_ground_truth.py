#!/usr/bin/env python3
"""Run a known query mix against Databend and collect aggregate telemetry.

Usage:
    python experiments/run_ground_truth.py \
        --mix experiments/mixes/easy.json \
        --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
        --database tpch20g \
        --concurrency 4 \
        --output experiments/results/
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
from pathlib import Path

import requests


def prom_query(prom_url: str, query: str) -> float:
    r = requests.get(f"{prom_url}/api/v1/query", params={"query": query})
    data = r.json()
    if data["status"] == "success" and data["data"]["result"]:
        return float(data["data"]["result"][0]["value"][1])
    return 0.0


def run_query(sql: str, database: str, host: str, port: int) -> dict:
    url = f"http://{host}:{port}/v1/query/"
    payload = {"sql": sql, "session": {"database": database}}
    start = time.time()
    try:
        resp = requests.post(url, json=payload, auth=("root", ""), timeout=600)
        elapsed = time.time() - start
        result = resp.json()
        error = result.get("error")
        if error:
            return {"status": "error", "error": str(error)[:200], "duration": elapsed}
        rows = len(result.get("data", []))
        return {"status": "ok", "duration": elapsed, "rows": rows}
    except Exception as e:
        return {"status": "error", "error": str(e)[:200], "duration": time.time() - start}


def main():
    p = argparse.ArgumentParser(description="Run ground truth query mix and collect telemetry")
    p.add_argument("--mix", required=True, help="Path to mix JSON file")
    p.add_argument("--metrics", required=True, help="Path to collected metrics JSON")
    p.add_argument("--database", default="tpch20g", help="Databend database name")
    p.add_argument("--host", default="localhost", help="Databend host")
    p.add_argument("--port", type=int, default=8000, help="Databend HTTP port")
    p.add_argument("--prom-url", default="http://localhost:9091", help="Prometheus URL")
    p.add_argument("--concurrency", type=int, default=1, help="Max concurrent queries")
    p.add_argument("--output", default="experiments/results/", help="Output directory")
    args = p.parse_args()

    with open(args.mix) as f:
        mix_data = json.load(f)

    mix_name = mix_data.get("name", Path(args.mix).stem)
    ground_truth = {int(k): v for k, v in mix_data["mix"].items()}

    with open(args.metrics) as f:
        all_queries = json.load(f)

    # Build execution list
    executions = []
    for idx, count in sorted(ground_truth.items()):
        q = all_queries[idx]["query"]
        if "@" in q:
            sql, _ = q.rsplit("@", 1)
        else:
            sql = q
        for _ in range(count):
            executions.append((sql.strip(), args.database, idx))

    n = len(executions)
    print(f"Mix: {mix_name} — {len(ground_truth)} distinct queries, {n} total executions")
    for idx, count in sorted(ground_truth.items()):
        q = all_queries[idx]
        print(f"  Q{idx+1} x{count}: cpu={q['avg_cpu_time']:.1f}s scan={q['avg_scan_bytes']/1e9:.3f}GB dur={q['avg_duration']:.3f}s")

    # Prometheus snapshot before
    cpu_before = prom_query(args.prom_url, "databend_query_duration_ms_sum") / 1000.0
    scan_before = prom_query(args.prom_url, "databend_query_scan_bytes_sum")

    # Execute
    print(f"\nRunning {n} queries (concurrency={args.concurrency})...")
    run_start = time.time()
    results = [None] * n

    if args.concurrency <= 1:
        for i, (sql, db, qidx) in enumerate(executions):
            r = run_query(sql, db, args.host, args.port)
            results[i] = r
            print(f"  [{i+1}/{n}] Q{qidx+1} {r['status']} dur={r['duration']:.3f}s")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = {}
            for i, (sql, db, qidx) in enumerate(executions):
                fut = ex.submit(run_query, sql, db, args.host, args.port)
                futures[fut] = (i, qidx)
            for fut in concurrent.futures.as_completed(futures):
                i, qidx = futures[fut]
                r = fut.result()
                results[i] = r
                done = sum(1 for x in results if x is not None)
                print(f"  [{done}/{n}] Q{qidx+1} {r['status']} dur={r['duration']:.3f}s")

    run_elapsed = time.time() - run_start
    ok_count = sum(1 for r in results if r["status"] == "ok")
    print(f"\nCompleted in {run_elapsed:.1f}s — {ok_count}/{n} succeeded")

    # Prometheus snapshot after (wait for scrape)
    print("Waiting 12s for Prometheus scrape...")
    time.sleep(12)
    cpu_after = prom_query(args.prom_url, "databend_query_duration_ms_sum") / 1000.0
    scan_after = prom_query(args.prom_url, "databend_query_scan_bytes_sum")

    total_cpu = cpu_after - cpu_before
    total_scan_bytes = scan_after - scan_before

    # Compute telemetry from per-query metrics (more reliable than Prometheus deltas
    # when Prometheus scan counters are unavailable)
    metrics_cpu = sum(all_queries[i]["avg_cpu_time"] * c for i, c in ground_truth.items())
    metrics_scan = sum(all_queries[i]["avg_scan_bytes"] * c / (1024**3) for i, c in ground_truth.items())
    metrics_dur = sum(all_queries[i]["avg_duration"] * c for i, c in ground_truth.items())
    client_dur = sum(r["duration"] for r in results if r["status"] == "ok")

    # Operator ratios
    total_f = sum(all_queries[i].get("filter", 0) * c for i, c in ground_truth.items())
    total_j = sum(all_queries[i].get("join", 0) * c for i, c in ground_truth.items())
    total_a = sum(all_queries[i].get("agg", 0) * c for i, c in ground_truth.items())
    total_s = sum(all_queries[i].get("sort", 0) * c for i, c in ground_truth.items())

    telemetry = {
        "name": mix_name,
        "n_queries": n,
        "n_distinct": len(ground_truth),
        "total_cpu_s": metrics_cpu,
        "total_scan_gb": metrics_scan,
        "avg_duration_s": metrics_dur / n,
        "filter_ratio": total_f / n,
        "join_ratio": total_j / n,
        "agg_ratio": total_a / n,
        "sort_ratio": total_s / n,
        "prometheus_cpu_delta_s": total_cpu,
        "prometheus_scan_delta_bytes": total_scan_bytes,
        "client_duration_sum_s": client_dur,
        "wall_clock_s": run_elapsed,
    }

    print(f"\n{'=' * 60}")
    print(f"OBSERVED TELEMETRY — {mix_name}")
    print(f"{'=' * 60}")
    print(f"  Total CPU:       {metrics_cpu:.2f}s")
    print(f"  Total Scan:      {metrics_scan:.4f}GB")
    print(f"  Avg Duration:    {metrics_dur / n:.3f}s")
    print(f"  Query count:     {n}")
    print(f"  Operators:       F={total_f/n:.3f} J={total_j/n:.3f} A={total_a/n:.3f} S={total_s/n:.3f}")
    print(f"  Prom CPU delta:  {total_cpu:.2f}s")
    print(f"  Client dur sum:  {client_dur:.2f}s")

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{mix_name}_telemetry.json"
    with open(out_path, "w") as f:
        json.dump(telemetry, f, indent=2)
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
