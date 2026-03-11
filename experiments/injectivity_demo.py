#!/usr/bin/env python3
"""Demonstrate that structurally different queries produce identical telemetry.

Runs pairs of queries (simple vs complex) and shows their Prometheus telemetry
is indistinguishable, proving PBench's 1:N mapping problem.
"""
import json
import time
import requests
import sys

DATABEND_URL = "http://localhost:8000/v1/query/"
PROM_URL = "http://localhost:9091"
DATABASE = "tpch20g"

# Query pairs: (name, sql, description)
PAIRS = [
    {
        "label": "Pair A: Simple scan vs 4-table analytical join",
        "q1": {
            "name": "Simple",
            "desc": "Single-table scan + filter + aggregate + sort",
            "sql": """
                SELECT l_returnflag, l_linestatus,
                       SUM(l_quantity) as sum_qty,
                       SUM(l_extendedprice) as sum_price
                FROM lineitem
                WHERE l_shipdate <= '1998-09-02'
                GROUP BY l_returnflag, l_linestatus
                ORDER BY l_returnflag, l_linestatus
            """,
            "tree": [
                "τ_{returnflag, linestatus}",
                "   |",
                "γ_{returnflag, linestatus, SUM(qty), SUM(price)}",
                "   |",
                "σ_{shipdate ≤ '1998-09-02'}",
                "   |",
                "LINEITEM  (6M rows full scan)",
            ]
        },
        "q2": {
            "name": "Complex",
            "desc": "4-table join + filter + aggregate + sort (supply chain revenue by nation)",
            "sql": """
                SELECT n_name,
                       SUM(l_extendedprice * (1 - l_discount)) as revenue
                FROM customer, orders, lineitem, nation
                WHERE c_custkey = o_custkey
                  AND l_orderkey = o_orderkey
                  AND c_nationkey = n_nationkey
                  AND l_shipdate >= '1994-01-01'
                  AND l_shipdate < '1995-01-01'
                GROUP BY n_name
                ORDER BY revenue DESC
            """,
            "tree": [
                "τ_{revenue DESC}",
                "   |",
                "γ_{n_name, SUM(price*(1-discount))}",
                "   |",
                "σ_{shipdate ∈ [1994, 1995)}",
                "   |",
                "⋈_{c_nationkey = n_nationkey}",
                "  /              \\",
                "⋈_{l_orderkey = o_orderkey}    NATION",
                "  /              \\",
                "⋈_{c_custkey = o_custkey}    LINEITEM",
                "  /              \\",
                "CUSTOMER          ORDERS",
            ]
        }
    },
]


def prom_query(query, ts=None):
    try:
        params = {"query": query}
        if ts is not None:
            params["time"] = ts
        r = requests.get(f"{PROM_URL}/api/v1/query", params=params, timeout=5)
        data = r.json()
        if data["status"] == "success" and data["data"]["result"]:
            return float(data["data"]["result"][0]["value"][1])
    except Exception as e:
        print(f"  [prom error] {e}")
    return 0.0


def run_query(sql):
    payload = {"sql": sql.strip(), "session": {"database": DATABASE}}
    start = time.time()
    resp = requests.post(DATABEND_URL, json=payload, auth=("root", ""), timeout=600)
    elapsed = time.time() - start
    result = resp.json()
    error = result.get("error")
    if error:
        print(f"  ERROR: {str(error)[:200]}")
        return None, elapsed
    rows = len(result.get("data", []))
    return rows, elapsed


def measure_query(name, sql):
    cpu_metric = "sum(databend_process_cpu_seconds_total_total)"
    scan_metric = 'sum(databend_query_scan_bytes_total{kind="Query"})'

    before_ts = time.time()
    cpu_before = prom_query(cpu_metric, ts=before_ts)
    scan_before = prom_query(scan_metric, ts=before_ts)

    rows, duration = run_query(sql)
    if rows is None:
        return None

    # Wait for Prometheus scrape
    time.sleep(12)

    after_ts = time.time()
    cpu_after = prom_query(cpu_metric, ts=after_ts)
    scan_after = prom_query(scan_metric, ts=after_ts)

    cpu_delta = cpu_after - cpu_before
    scan_delta = (scan_after - scan_before) / (1024**3)

    return {
        "name": name,
        "cpu_s": cpu_delta,
        "scan_gb": scan_delta,
        "duration_s": duration,
        "rows": rows,
    }


def main():
    print("=" * 70)
    print("INJECTIVITY DEMONSTRATION")
    print("Different queries → Same telemetry")
    print("=" * 70)

    results = []

    for pair in PAIRS:
        print(f"\n{'─' * 70}")
        print(f"{pair['label']}")
        print(f"{'─' * 70}")

        for qkey in ["q1", "q2"]:
            q = pair[qkey]
            print(f"\n  [{q['name']}] {q['desc']}")
            print(f"  Relational algebra:")
            for line in q["tree"]:
                print(f"    {line}")
            print(f"\n  Running...")

            m = measure_query(q["name"], q["sql"])
            if m is None:
                print("  FAILED — skipping")
                continue

            print(f"  Results: {m['rows']} rows in {m['duration_s']:.3f}s")
            print(f"  ┌─────────────────────────────────────┐")
            print(f"  │  CPU:      {m['cpu_s']:>8.1f} s              │")
            print(f"  │  Scan:     {m['scan_gb']:>8.4f} GB            │")
            print(f"  │  Duration: {m['duration_s']:>8.3f} s             │")
            print(f"  │  Operators: F=1 J={'1' if qkey == 'q2' else '0'} A=1 S=1          │")
            print(f"  └─────────────────────────────────────┘")
            results.append(m)

        if len(results) >= 2:
            r1, r2 = results[-2], results[-1]
            print(f"\n  {'COMPARISON':^50}")
            print(f"  {'─' * 50}")
            print(f"  {'Metric':<15} {'Simple':>12} {'Complex':>12} {'Diff':>10}")
            print(f"  {'─' * 50}")
            print(f"  {'CPU (s)':<15} {r1['cpu_s']:>12.1f} {r2['cpu_s']:>12.1f} {abs(r1['cpu_s']-r2['cpu_s']):>9.1f}s")
            print(f"  {'Scan (GB)':<15} {r1['scan_gb']:>12.4f} {r2['scan_gb']:>12.4f} {abs(r1['scan_gb']-r2['scan_gb']):>9.4f}")
            print(f"  {'Duration (s)':<15} {r1['duration_s']:>12.3f} {r2['duration_s']:>12.3f} {abs(r1['duration_s']-r2['duration_s']):>9.3f}")
            cpu_match = "✓ SAME" if abs(r1['cpu_s'] - r2['cpu_s']) <= 2 else "✗ DIFF"
            scan_match = "✓ SAME" if abs(r1['scan_gb'] - r2['scan_gb']) < 1 else "✗ DIFF"
            print(f"\n  Telemetry match: CPU={cpu_match}  Scan={scan_match}")
            if cpu_match == "✓ SAME" and scan_match == "✓ SAME":
                print(f"  → PBench CANNOT distinguish these queries from telemetry alone")

    # Save
    out = {"pairs": results}
    with open("experiments/results/injectivity_demo.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved: experiments/results/injectivity_demo.json")


if __name__ == "__main__":
    main()
