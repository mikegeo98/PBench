#!/usr/bin/env python3
"""
Create a workload CSV from TPC-H metrics JSON, suitable for llm_gen.py's main().
The CSV format matches what create_perf_goal() expects:
  databaseid, qminute, cputime_sum, scanbytes_sum, avg_durationtime,
  avg_memoryused, join, agg, sort, filter, proj,
  cputime_interval, scanbytes_interval, duration_interval, memory_interval,
  filter_interval, sort_interval, agg_interval, join_interval
"""
import json
import sys
import os

def main():
    metrics_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "src", "Collect_metrics", "metrics_witho", "output",
        "TPCH-tpch20g-sql-metrics.json"
    )
    output_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(__file__), "..", "src", "Workloads", "tpch20g_workload.csv"
    )

    with open(metrics_path) as f:
        records = json.load(f)

    # Aggregate all queries into a single "5-minute window" row
    total_cpu = sum(r["avg_cpu_time"] for r in records)
    total_scan = sum(r["avg_scan_bytes"] for r in records)
    avg_duration = sum(r.get("avg_duration", 0) for r in records) / len(records)
    n = len(records)

    # Operator ratios as probabilities (fraction of queries that have each operator)
    filter_ratio = sum(1 for r in records if r.get("filter", 0) > 0) / n
    join_ratio = sum(1 for r in records if r.get("join", 0) > 0) / n
    agg_ratio = sum(1 for r in records if r.get("agg", 0) > 0) / n
    sort_ratio = sum(1 for r in records if r.get("sort", 0) > 0) / n

    # Create 10-slot interval arrays (spread evenly)
    cpu_per_slot = total_cpu / 10
    scan_per_slot = total_scan / (1024**3) / 10  # convert to GB
    dur_per_slot = avg_duration
    mem_per_slot = 0.1

    cpu_interval = [round(cpu_per_slot, 2)] * 10
    scan_interval = [round(scan_per_slot, 4)] * 10
    dur_interval = [round(dur_per_slot, 4)] * 10
    mem_interval = [round(mem_per_slot, 4)] * 10
    filter_interval = [round(filter_ratio, 4)] * 10
    sort_interval = [round(sort_ratio, 4)] * 10
    agg_interval = [round(agg_ratio, 4)] * 10
    join_interval = [round(join_ratio, 4)] * 10

    def fmt_list(lst):
        return '"[' + ", ".join(str(x) for x in lst) + ']"'

    header = "databaseid,qminute,cputime_sum,scanbytes_sum,avg_durationtime,avg_memoryused,join,agg,sort,filter,proj,cputime_interval,scanbytes_interval,duration_interval,memory_interval,filter_interval,sort_interval,agg_interval,join_interval"

    # scan in GB for the CSV
    total_scan_gb = total_scan / (1024**3)

    row = (
        f"1234567890,2026-01-01 00:00:00,"
        f"{round(total_cpu, 2)},{round(total_scan_gb, 4)},{round(avg_duration, 4)},0.2,"
        f"{round(join_ratio, 4)},{round(agg_ratio, 4)},{round(sort_ratio, 4)},{round(filter_ratio, 4)},0.0,"
        f"{fmt_list(cpu_interval)},{fmt_list(scan_interval)},{fmt_list(dur_interval)},{fmt_list(mem_interval)},"
        f"{fmt_list(filter_interval)},{fmt_list(sort_interval)},{fmt_list(agg_interval)},{fmt_list(join_interval)}"
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(header + "\n")
        f.write(row + "\n")

    print(f"Written to {output_path}")
    print(f"  Total CPU: {total_cpu}s, Total Scan: {round(total_scan_gb, 2)} GB")
    print(f"  Operator ratios: F={filter_ratio:.2f} J={join_ratio:.2f} A={agg_ratio:.2f} S={sort_ratio:.2f}")
    print(f"  Queries: {n}")


if __name__ == "__main__":
    main()
