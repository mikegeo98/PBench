#!/usr/bin/env python3
"""Feed observed telemetry to PBench's ILP solver and compare against ground truth.

Usage:
    python experiments/recover_mix.py \
        --telemetry experiments/results/easy_telemetry.json \
        --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
        --secret experiments/mixes/easy.json \
        --count-limit 30
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Add PBench-tool to path for ILP solver
PBENCH_TOOL = Path(__file__).resolve().parent.parent / "src" / "PBench-tool"
if str(PBENCH_TOOL) not in sys.path:
    sys.path.insert(0, str(PBENCH_TOOL))


def main():
    p = argparse.ArgumentParser(description="Recover query mix from telemetry using PBench ILP")
    p.add_argument("--telemetry", required=True, help="Telemetry JSON from run_ground_truth.py")
    p.add_argument("--metrics", required=True, help="Collected metrics JSON (candidate pool)")
    p.add_argument("--secret", required=True, help="Secret mix JSON for comparison")
    p.add_argument("--count-limit", type=int, default=30, help="Max total queries for ILP")
    p.add_argument("--op-scale", type=int, default=100, help="Operator weight scale in objective")
    args = p.parse_args()

    # Must chdir so PBench-tool relative imports work
    orig_dir = os.getcwd()
    os.chdir(str(PBENCH_TOOL))

    from linearprogram_option import solve_integer_linear_programming_cycle

    os.chdir(orig_dir)

    with open(args.telemetry) as f:
        telemetry = json.load(f)
    with open(args.metrics) as f:
        all_queries = json.load(f)
    with open(args.secret) as f:
        secret_data = json.load(f)

    ground_truth = {int(k): v for k, v in secret_data["mix"].items()}
    mix_name = telemetry.get("name", "unknown")
    n = telemetry["n_queries"]

    # Build candidate pool
    candidates = []
    for q in all_queries:
        entry = dict(q)
        entry["avg_scan_bytes"] = entry["avg_scan_bytes"] / (1024**3)
        if "@" not in entry["query"]:
            entry["query"] += "@tpch20g"
        for op in ["filter", "join", "agg", "sort"]:
            entry.setdefault(op, 0)
        candidates.append(entry)

    config = {
        "use_operator": 1,
        "op_scale": args.op_scale,
        "count_limit": args.count_limit,
        "time_limit": telemetry["avg_duration_s"] * n * 2,
        "initial_count": n,
    }

    print(f"{'=' * 65}")
    print(f"ILP RECOVERY — {mix_name}")
    print(f"{'=' * 65}")
    print(f"  Target: CPU={telemetry['total_cpu_s']:.2f}s  Scan={telemetry['total_scan_gb']:.4f}GB"
          f"  AvgDur={telemetry['avg_duration_s']:.3f}s  N={n}")
    print(f"  Ops:    F={telemetry['filter_ratio']:.3f} J={telemetry['join_ratio']:.3f}"
          f" A={telemetry['agg_ratio']:.3f} S={telemetry['sort_ratio']:.3f}")
    print(f"  Pool:   {len(candidates)} queries, count_limit={args.count_limit}\n")

    min_diff, solution = solve_integer_linear_programming_cycle(
        config,
        candidates,
        target_cpu_time=telemetry["total_cpu_s"],
        target_scan_bytes=telemetry["total_scan_gb"],
        target_duration=telemetry["avg_duration_s"],
        time_limit=config["time_limit"],
        target_filter=telemetry["filter_ratio"],
        target_join=telemetry["join_ratio"],
        target_agg=telemetry["agg_ratio"],
        target_sort=telemetry["sort_ratio"],
        count_limit=config["count_limit"],
        init_count=config["initial_count"],
    )

    # Compare
    print(f"\n{'=' * 65}")
    print(f"RESULTS — {mix_name}")
    print(f"{'=' * 65}")
    print(f"\n{'Query':<8} {'Truth':>6} {'Guess':>6} {'Match':>6}  {'cpu(s)':>8} {'scan(GB)':>9} {'dur(s)':>8}")
    print("-" * 65)

    exact = close = wrong = 0
    total_truth = total_guess = 0

    for i in range(len(all_queries)):
        truth = ground_truth.get(i, 0)
        guess = int(solution[i]) if i < len(solution) else 0
        if truth > 0 or guess > 0:
            q = all_queries[i]
            if truth == guess:
                m = "✓"
                exact += 1
            elif abs(truth - guess) <= 1:
                m = "~"
                close += 1
            else:
                m = "✗"
                wrong += 1
            print(f"Q{i+1:<7} {truth:>6} {guess:>6} {m:>6}  {q['avg_cpu_time']:>8.1f} {q['avg_scan_bytes']/1e9:>9.3f} {q['avg_duration']:>8.3f}")
            total_truth += truth
            total_guess += guess

    print("-" * 65)
    print(f"{'Total':<8} {total_truth:>6} {total_guess:>6}")

    # Aggregate telemetry comparison
    guess_cpu = sum(c["avg_cpu_time"] * s for c, s in zip(candidates, solution))
    guess_scan = sum(c["avg_scan_bytes"] * s for c, s in zip(candidates, solution))
    guess_dur = sum(c["avg_duration"] * s for c, s in zip(candidates, solution))

    real_cpu = telemetry["total_cpu_s"]
    real_scan = telemetry["total_scan_gb"]
    real_dur = telemetry["avg_duration_s"] * n

    print(f"\n{'Metric':<15} {'Truth':>10} {'Guess':>10} {'Error':>8}")
    print("-" * 48)
    print(f"{'CPU (s)':<15} {real_cpu:>10.2f} {guess_cpu:>10.2f} {abs(real_cpu-guess_cpu)/max(real_cpu,0.01)*100:>7.1f}%")
    print(f"{'Scan (GB)':<15} {real_scan:>10.4f} {guess_scan:>10.4f} {abs(real_scan-guess_scan)/max(real_scan,0.001)*100:>7.1f}%")
    print(f"{'Duration (s)':<15} {real_dur:>10.3f} {guess_dur:>10.3f} {abs(real_dur-guess_dur)/max(real_dur,0.01)*100:>7.1f}%")
    print(f"{'Count':<15} {total_truth:>10} {int(sum(solution)):>10}")

    n_compared = exact + close + wrong
    print(f"\nExact: {exact}/{n_compared}, Close(±1): {close}/{n_compared}, Wrong: {wrong}/{n_compared}")
    print(f"Objective: {min_diff:.4f}")

    # Save results
    result = {
        "name": mix_name,
        "ground_truth": {f"Q{k+1}": v for k, v in ground_truth.items()},
        "ilp_guess": {f"Q{i+1}": int(s) for i, s in enumerate(solution) if s > 0},
        "exact_matches": exact,
        "close_matches": close,
        "wrong_matches": wrong,
        "objective": min_diff,
        "cpu_error_pct": abs(real_cpu - guess_cpu) / max(real_cpu, 0.01) * 100,
        "scan_error_pct": abs(real_scan - guess_scan) / max(real_scan, 0.001) * 100,
    }

    out_path = Path(args.telemetry).parent / f"{mix_name}_recovery.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
