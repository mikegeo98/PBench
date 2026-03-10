#!/usr/bin/env python3
"""Convert groundtruth output_events.jsonl to a mix JSON for ILP recovery.

Reads the JSONL events produced by groundtruth/run_bench.py and counts how
many times each query template was executed. Maps template_id to 0-indexed
query index for the ILP solver.

Usage:
    python experiments/events_to_mix.py \
        --events groundtruth/output/tpch20g-seq/output_events.jsonl \
        --output experiments/mixes/groundtruth_seq.json
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Convert groundtruth events to mix JSON")
    p.add_argument("--events", required=True, help="Path to output_events.jsonl")
    p.add_argument("--output", required=True, help="Output mix JSON path")
    p.add_argument("--name", default=None, help="Mix name (default: derived from events path)")
    args = p.parse_args()

    events = []
    with open(args.events) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    if not events:
        print(f"No events found in {args.events}")
        return

    # Count by template_id
    template_counts = Counter()
    for ev in events:
        tid = ev.get("template_id", ev.get("event_id", "unknown"))
        template_counts[tid] += 1

    # Try to map template_id to 0-indexed query number.
    # Common patterns: "0", "1", ... or "q1", "q2", ... or "e0000000", ...
    mix = {}
    for tid, count in sorted(template_counts.items()):
        # Try numeric parse
        try:
            idx = int(tid)
            mix[str(idx)] = count
            continue
        except (ValueError, TypeError):
            pass
        # Try stripping prefix like "q" or "Q"
        stripped = str(tid).lstrip("qQe")
        try:
            idx = int(stripped)
            mix[str(idx)] = count
            continue
        except (ValueError, TypeError):
            pass
        # Fall back to using template_id as-is
        mix[str(tid)] = count

    name = args.name or Path(args.events).parent.name
    n_ok = sum(1 for ev in events if ev.get("status") == "ok")

    result = {
        "name": name,
        "description": f"Groundtruth run: {len(events)} events ({n_ok} ok), {len(mix)} distinct templates",
        "mix": mix,
        "source": str(args.events),
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Events: {len(events)} ({n_ok} ok)")
    print(f"Templates: {len(mix)} distinct")
    for tid, count in sorted(mix.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
        print(f"  template {tid}: {count}x")
    print(f"\nSaved: {args.output}")


if __name__ == "__main__":
    main()
