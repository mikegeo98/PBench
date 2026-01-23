#!/usr/bin/env python3
"""Test metrics collection on a few sample queries."""
import json
import time
from pathlib import Path
import requests


def execute_query(host: str, port: int, query: str, database: str):
    """Execute a query against Databend."""
    url = f"http://{host}:{port}/v1/query/"
    headers = {"Content-Type": "application/json"}

    # First set the database
    requests.post(url, headers=headers, auth=("root", ""),
                  json={"sql": f"USE {database}"})

    # Add EXPLAIN ANALYZE
    if not query.upper().startswith("EXPLAIN"):
        query = f"EXPLAIN ANALYZE {query}"

    response = requests.post(url, headers=headers, auth=("root", ""),
                             json={"sql": query})
    return response.json()


def count_operators(plan_data: list) -> dict:
    """Count operators in EXPLAIN ANALYZE output."""
    operators = {"filter": 0, "join": 0, "agg": 0, "sort": 0}
    keywords = {
        "filter": "Filter",
        "join": "HashJoin",
        "agg": "AggregateFinal",
        "sort": "Sort"
    }

    plan_text = "\n".join([str(row) for row in plan_data])

    for op, keyword in keywords.items():
        operators[op] = plan_text.count(keyword)

    return operators


def test_query(query: str, database: str, host: str = "localhost", port: int = 8000):
    """Test a single query and collect metrics."""
    start_time = time.time()
    result = execute_query(host, port, query, database)
    end_time = time.time()

    duration = end_time - start_time

    if result.get("error"):
        return {
            "success": False,
            "error": result["error"]["message"][:200],
            "duration": duration
        }

    # Get operator counts from plan
    plan_data = result.get("data", [])
    operators = count_operators(plan_data)

    # Get stats
    stats = result.get("stats", {})
    scan_progress = stats.get("scan_progress", {})
    cpu_time = stats.get("running_time_ms", 0) / 1000

    return {
        "success": True,
        "duration": duration,
        "cpu_time_ms": stats.get("running_time_ms", 0),
        "scan_bytes": scan_progress.get("bytes", 0),
        "rows_scanned": scan_progress.get("rows", 0),
        **operators
    }


def main():
    # Test TPC-H queries
    tpch_queries = [
        ("TPC-H Q1", """
            SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty
            FROM lineitem
            WHERE l_shipdate <= '1998-12-01'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """),
        ("TPC-H Q3 (simplified)", """
            SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer, orders, lineitem
            WHERE c_mktsegment = 'BUILDING'
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
            GROUP BY l_orderkey
            ORDER BY revenue DESC
            LIMIT 10
        """),
        ("TPC-H Q6", """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= '1994-01-01'
              AND l_shipdate < '1995-01-01'
              AND l_discount BETWEEN 0.05 AND 0.07
              AND l_quantity < 24
        """),
    ]

    # Test IMDB/JOB queries
    job_queries = [
        ("JOB 10a (simplified)", """
            SELECT MIN(t.title) AS movie
            FROM title AS t, cast_info AS ci, role_type AS rt
            WHERE t.id = ci.movie_id
              AND ci.role_id = rt.id
              AND rt.role = 'actor'
        """),
    ]

    print("=" * 70)
    print("Testing TPC-H Queries (database: tpch1g)")
    print("=" * 70)

    for name, query in tpch_queries:
        print(f"\n{name}:")
        result = test_query(query, "tpch1g")
        if result["success"]:
            print(f"  ✓ Duration: {result['duration']:.3f}s")
            print(f"    CPU time: {result['cpu_time_ms']}ms")
            print(f"    Scan: {result['scan_bytes']} bytes, {result['rows_scanned']} rows")
            print(f"    Operators: filter={result['filter']}, join={result['join']}, "
                  f"agg={result['agg']}, sort={result['sort']}")
        else:
            print(f"  ✗ Error: {result['error']}")

    print("\n" + "=" * 70)
    print("Testing JOB Queries (database: imdb)")
    print("=" * 70)

    for name, query in job_queries:
        print(f"\n{name}:")
        result = test_query(query, "imdb")
        if result["success"]:
            print(f"  ✓ Duration: {result['duration']:.3f}s")
            print(f"    CPU time: {result['cpu_time_ms']}ms")
            print(f"    Scan: {result['scan_bytes']} bytes, {result['rows_scanned']} rows")
            print(f"    Operators: filter={result['filter']}, join={result['join']}, "
                  f"agg={result['agg']}, sort={result['sort']}")
        else:
            print(f"  ✗ Error: {result['error']}")

    print("\n" + "=" * 70)
    print("Metrics collection test complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
