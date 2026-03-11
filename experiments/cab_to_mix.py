#!/usr/bin/env python3
"""Extract a time window from a CAB workload stream and convert to a mix JSON.

CAB streams use 1-indexed TPC-H query IDs. This script converts to 0-indexed
(Q1=0, Q2=1, ..., Q22=21) for the ILP solver.

Usage:
    python experiments/cab_to_mix.py \
        --stream groundtruth/cab/query_stream_3.json \
        --minutes 5 \
        --output experiments/mixes/cab_stream3_5min.json
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Extract CAB stream window as mix JSON")
    p.add_argument("--stream", required=True, help="Path to CAB query_stream_*.json")
    p.add_argument("--minutes", type=float, default=5, help="Window duration in minutes (default: 5)")
    p.add_argument("--offset-minutes", type=float, default=0, help="Start offset in minutes (default: 0 = from first query)")
    p.add_argument("--output", required=True, help="Output mix JSON path")
    args = p.parse_args()

    with open(args.stream) as f:
        data = json.load(f)

    queries = data["queries"]
    starts = [q["start"] for q in queries]
    min_start = min(starts)

    window_start = min_start + args.offset_minutes * 60_000
    window_end = window_start + args.minutes * 60_000

    window = [q for q in queries if window_start <= q["start"] <= window_end]

    # Count by query_id (1-indexed TPC-H number)
    cab_counts = Counter(q["query_id"] for q in window)

    # Convert to 0-indexed, skip any IDs > 22
    mix = {}
    skipped = []
    for qid, count in sorted(cab_counts.items()):
        if 1 <= qid <= 22:
            mix[str(qid - 1)] = count  # 1-indexed -> 0-indexed
        else:
            skipped.append(qid)

    stream_name = Path(args.stream).stem
    name = f"cab_{stream_name}_{int(args.minutes)}min"

    result = {
        "name": name,
        "description": f"First {args.minutes} min of {stream_name} ({len(window)} queries, {len(mix)} distinct TPC-H templates)",
        "mix": mix,
        "source": str(args.stream),
        "window_ms": [int(window_start), int(window_end)],
    }

    if skipped:
        result["skipped_query_ids"] = sorted(skipped)
        print(f"Warning: skipped non-TPC-H query IDs: {sorted(skipped)}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Stream: {stream_name}")
    print(f"Window: {args.minutes} min (offset {args.offset_minutes} min)")
    print(f"Queries: {len(window)} total, {len(mix)} distinct TPC-H templates")
    for qid_0, count in sorted(mix.items(), key=lambda x: int(x[0])):
        print(f"  Q{int(qid_0)+1}: {count}x")
    print(f"\nSaved: {args.output}")


if __name__ == "__main__":
    main()
