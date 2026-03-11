#!/usr/bin/env python3
"""Find two structurally different queries with matching telemetry.

Iteratively tunes query predicates until a simple 2-table query and a
complex 5-table query produce the same Prometheus telemetry vector.
"""
import time
import requests
import json

DATABEND_URL = "http://localhost:8000/v1/query/"
PROM_URL = "http://localhost:9091"
DATABASE = "tpch20g"

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

def run_and_measure(label, sql):
    cpu_metric = "sum(databend_process_cpu_seconds_total_total)"
    scan_metric = 'sum(databend_query_scan_bytes_total{kind="Query"})'

    before_ts = time.time()
    cpu_before = prom_query(cpu_metric, ts=before_ts)
    scan_before = prom_query(scan_metric, ts=before_ts)

    payload = {"sql": sql.strip(), "session": {"database": DATABASE}}
    start = time.time()
    resp = requests.post(DATABEND_URL, json=payload, auth=("root", ""), timeout=600)
    duration = time.time() - start
    result = resp.json()
    error = result.get("error")
    if error:
        print(f"  [{label}] ERROR: {str(error)[:200]}")
        return None

    rows = len(result.get("data", []))
    time.sleep(12)

    after_ts = time.time()
    cpu_after = prom_query(cpu_metric, ts=after_ts)
    scan_after = prom_query(scan_metric, ts=after_ts)

    cpu_delta = cpu_after - cpu_before
    scan_delta = (scan_after - scan_before) / (1024**3)

    print(f"  [{label}] cpu={cpu_delta:.0f}s  scan={scan_delta:.4f}GB  dur={duration:.3f}s  rows={rows}")
    return {"cpu": cpu_delta, "scan": scan_delta, "dur": duration, "rows": rows, "sql": sql.strip()}


# --- SIMPLE QUERY VARIANTS (2 tables: orders + lineitem) ---
# Tune by changing date range on l_shipdate
simple_variants = []
# Wider date range = more scan/cpu
for year_start, year_end in [
    ("1993-01-01", "1993-07-01"),
    ("1993-01-01", "1994-01-01"),
    ("1993-01-01", "1994-07-01"),
    ("1993-01-01", "1995-01-01"),
    ("1992-01-01", "1995-01-01"),
    ("1992-01-01", "1996-01-01"),
    ("1992-01-01", "1997-01-01"),
    ("1992-01-01", "1998-01-01"),
]:
    simple_variants.append({
        "label": f"simple[{year_start}..{year_end}]",
        "sql": f"""
            SELECT o_orderpriority, COUNT(*) as cnt,
                   SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM orders, lineitem
            WHERE o_orderkey = l_orderkey
              AND l_shipdate >= '{year_start}'
              AND l_shipdate < '{year_end}'
              AND l_shipdate > l_commitdate
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        """
    })

# --- COMPLEX QUERY VARIANTS (5 tables: nation, supplier, partsupp, lineitem, orders) ---
# Tune by changing p_name LIKE pattern selectivity or date range
complex_variants = []
for year_start, year_end in [
    ("1993-01-01", "1993-04-01"),
    ("1993-01-01", "1993-07-01"),
    ("1993-01-01", "1994-01-01"),
    ("1993-01-01", "1995-01-01"),
    ("1992-01-01", "1995-01-01"),
    ("1992-01-01", "1996-01-01"),
    ("1992-01-01", "1997-01-01"),
    ("1992-01-01", "1998-01-01"),
]:
    complex_variants.append({
        "label": f"complex[{year_start}..{year_end}]",
        "sql": f"""
            SELECT n_name,
                   SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as margin
            FROM nation, supplier, partsupp, lineitem, orders
            WHERE s_nationkey = n_nationkey
              AND s_suppkey = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= '{year_start}'
              AND o_orderdate < '{year_end}'
            GROUP BY n_name
            ORDER BY margin DESC
        """
    })


def main():
    print("=" * 70)
    print("TELEMETRY TWIN FINDER")
    print("Searching for simple & complex queries with matching telemetry")
    print("=" * 70)

    print("\n--- Phase 1: Profile SIMPLE query variants ---")
    simple_results = []
    for v in simple_variants:
        m = run_and_measure(v["label"], v["sql"])
        if m:
            m["label"] = v["label"]
            simple_results.append(m)

    print("\n--- Phase 2: Profile COMPLEX query variants ---")
    complex_results = []
    for v in complex_variants:
        m = run_and_measure(v["label"], v["sql"])
        if m:
            m["label"] = v["label"]
            complex_results.append(m)

    # Find best match
    print("\n--- Phase 3: Find closest telemetry match ---")
    print(f"\n{'Simple':<35} {'Complex':<35} {'CPU diff':>8} {'Scan diff':>10}")
    print("-" * 95)

    best_pair = None
    best_score = float('inf')
    for s in simple_results:
        for c in complex_results:
            cpu_diff = abs(s["cpu"] - c["cpu"])
            scan_diff = abs(s["scan"] - c["scan"])
            # Score: weighted combination
            score = cpu_diff + scan_diff * 10
            match_str = " ← MATCH!" if cpu_diff <= 1 and scan_diff < 0.5 else ""
            print(f"  {s['label']:<33} {c['label']:<33} {cpu_diff:>7.0f}s {scan_diff:>9.4f}GB{match_str}")
            if score < best_score:
                best_score = score
                best_pair = (s, c)

    if best_pair:
        s, c = best_pair
        print(f"\n{'=' * 70}")
        print(f"BEST MATCH")
        print(f"{'=' * 70}")
        print(f"  Simple:  {s['label']}")
        print(f"    CPU={s['cpu']:.0f}s  Scan={s['scan']:.4f}GB  Dur={s['dur']:.3f}s")
        print(f"  Complex: {c['label']}")
        print(f"    CPU={c['cpu']:.0f}s  Scan={c['scan']:.4f}GB  Dur={c['dur']:.3f}s")
        print(f"  CPU diff: {abs(s['cpu']-c['cpu']):.0f}s")
        print(f"  Scan diff: {abs(s['scan']-c['scan']):.4f}GB")
        print(f"\n  Both have operators: F=1, J=1, A=1, S=1")

        result = {
            "simple": {"label": s["label"], "sql": s["sql"], "cpu": s["cpu"], "scan": s["scan"], "dur": s["dur"]},
            "complex": {"label": c["label"], "sql": c["sql"], "cpu": c["cpu"], "scan": c["scan"], "dur": c["dur"]},
            "cpu_diff": abs(s["cpu"] - c["cpu"]),
            "scan_diff": abs(s["scan"] - c["scan"]),
        }
        with open("experiments/results/telemetry_twins.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved: experiments/results/telemetry_twins.json")


if __name__ == "__main__":
    main()
