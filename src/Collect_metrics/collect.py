import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
import sys

from databend_py import Client
from dotenv import load_dotenv

# Make sure the repo's `src` directory is on the import path so we can reuse utils.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.prometheus import prometheus_queries

# Benchmark configurations
BENCHMARKS = {
    "tpch": {
        "input": "./metrics_witho/input/TPCH-tpch1g-sql-input.json",
        "output": "./metrics_witho/output/TPCH-tpch1g-sql-metrics.json",
        "description": "TPC-H benchmark (22 queries)"
    },
    "imdb": {
        "input": "./metrics_witho/input/imdb-imdb-sql-input.json",
        "output": "./metrics_witho/output/imdb-imdb-sql-metrics.json",
        "description": "IMDB/JOB benchmark (113 queries)"
    },
    "tpcds": {
        "input": "./metrics_witho/input/tpcds_all-tpcds1g-sql-input.json",
        "output": "./metrics_witho/output/tpcds_all-tpcds1g-sql-metrics.json",
        "description": "TPC-DS benchmark"
    }
}


def get_time():
    """Return a timestamp (seconds since epoch)."""
    return time.time()


def load_config():
    """ Load configuration from environment variable. """
    load_dotenv()
    return {
        "host": os.getenv("HOST"),
        "databend_port": os.getenv("DATABEND_PORT"),
        "prometheus_port": os.getenv("PROMETHEUS_PORT"),
        "query": os.getenv("LP_QUERY_SET", "").split(","),
        "db": os.getenv("LP_DATABASE", "").split(","),
        "wait": int(os.getenv("WAIT_TIME", 0))
    }


def load_query_set(query_set):
    """ Load SQL statements from a file. """
    sql_statements = []
    query_path = os.path.join(os.path.dirname(__file__), "query", f"{query_set}.sql")
    current_statement = ""
    sort_key = None

    with open(query_path, "r") as file:
        for line in file:
            if line.startswith(" '.SQL/"):
                if current_statement:
                    sql_statements.append((sort_key, current_statement.strip()))
                    current_statement = ""
                sort_key = int(re.search(r"\.SQL/(\d+)\.0", line).group(1))
            else:
                current_statement += line.strip() + " "

    if current_statement:
        sql_statements.append((sort_key, current_statement.strip()))
    # sql_statements.sort(key=lambda x: x[0])
    sorted_sql_statements = [stmt[1] for stmt in sql_statements]
    
    return sorted_sql_statements

def load_query_from_json(path):
    with open(path, "r") as file:
        return json.load(file)


def execute_query(host, port, query, database, explain_analyze=False):
    # if there are more than one query in the file, only execute them one by one
    query = query.split(";")
    # make sure the last query is not empty
    if query[-1] == "":
        query = query[:-1]
    # add the last semicolon to each query
    query = [q + ";" for q in query]
    ret = []
    for q in query:
        if explain_analyze and not q.upper().startswith("EXPLAIN ANALYZE"):
            q = "EXPLAIN ANALYZE " + q
        client = Client(f"root:@{host}", port=port, secure=False, database=database)
        print(f"  Executing: {q[:80]}...")
        try:
            tmp = client.execute(q)
            ret.append(tmp)
        except Exception as e:
            print(f"Error: {e}")
            pass
    return ret

def record_metrics(host, databend_port, prometheus_port, query, wait_time, database):
    """ Record and print metrics related to the executed query. """
    # Wait for fresh Prometheus scrape before starting (scrape interval is 5s)
    time.sleep(6)

    start_time = get_time()
    print(f"  Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    start_cputime = prometheus_queries["cpu"](host, prometheus_port, start_time)
    start_scan = prometheus_queries["scan"](host, prometheus_port, start_time)
    print(f"    Start - CPU: {start_cputime}, Scan: {start_scan}")

    # Run the actual query (not EXPLAIN ANALYZE) to capture real metrics
    query_start = get_time()
    execute_query(host, databend_port, query, database, explain_analyze=False)
    query_duration = get_time() - query_start

    # Wait for Prometheus to scrape new metrics
    time.sleep(6)

    end_time = get_time()
    print(f"  End time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    end_cputime = prometheus_queries["cpu"](host, prometheus_port, end_time)
    end_scan = prometheus_queries["scan"](host, prometheus_port, end_time)
    print(f"    End   - CPU: {end_cputime}, Scan: {end_scan}")

    return query, end_cputime - start_cputime, end_scan - start_scan, query_duration


def save_data_to_file(data, record_file):
    """ Save data to a file. """
    with open(record_file, "w") as file:
        json.dump(data, file, indent=2)


def record_operator(host, databend_port, query, database):
    """ Record the operators used in the query. """
    operator_keywords = {
        # Databend plans often show pushdown filters as "filters:" in TableScan nodes.
        "filter": "filters:",
        "join": "HashJoin",
        "agg": "AggregateFinal",
        "sort": "Sort"
    }
    # Use EXPLAIN ANALYZE to get the query plan for operator detection
    plan = execute_query(host, databend_port, query, database, explain_analyze=True)
    operator_flag = {}
    for i in range(len(plan)):
        # databend-py returns (result_rows, plan_rows) for EXPLAIN ANALYZE in recent versions.
        plan_rows = plan[i][1] if len(plan[i]) > 1 else []
        if not plan_rows:
            print("Warning: no plan rows returned for this query {plan[i]}; operator flags may be incomplete.")
        tmp = '\n'.join([row[0] for row in plan_rows if row])
        for operator, keyword in operator_keywords.items():
            if keyword in tmp:
                operator_flag[operator] = 1
            else:
                operator_flag[operator] = 0
    return operator_flag


def main():
    parser = argparse.ArgumentParser(
        description="Collect metrics for database benchmark queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available benchmarks:
  tpch   - TPC-H benchmark (22 queries on tpch1g database)
  imdb   - IMDB/JOB benchmark (113 queries on imdb database)
  tpcds  - TPC-DS benchmark (on tpcds1g database)

Examples:
  python collect.py tpch          # Collect TPC-H metrics
  python collect.py imdb          # Collect IMDB metrics
  python collect.py imdb --repeat 5  # Run each query 5 times
        """
    )
    parser.add_argument(
        "benchmark",
        choices=list(BENCHMARKS.keys()),
        help="Benchmark to run (tpch, imdb, or tpcds)"
    )
    parser.add_argument(
        "--repeat", "-r",
        type=int,
        default=3,
        help="Number of times to repeat each query (default: 3)"
    )
    parser.add_argument(
        "--start", "-s",
        type=int,
        default=None,
        help="Start from query index (0-based, overrides resume)"
    )
    args = parser.parse_args()

    config = load_config()
    benchmark = BENCHMARKS[args.benchmark]

    src = benchmark["input"]
    record_file = benchmark["output"]
    repeat = args.repeat

    print(f"Benchmark: {args.benchmark.upper()} - {benchmark['description']}")
    print(f"Input: {src}")
    print(f"Output: {record_file}")
    print(f"Repeat: {repeat}x per query")
    print("=" * 60)

    print(f"\nLoading queries from: {src}")
    sql_statements = load_query_from_json(src)
    print(f"Found {len(sql_statements)} queries")
    data = []

    # Resume from existing progress or start fresh
    if args.start is not None:
        start_index = args.start
        print(f"Starting from query index {start_index} (as specified)")
    elif os.path.exists(record_file):
        with open(record_file, "r") as file:
            data = json.load(file)
        start_index = len(data)
        if start_index > 0:
            print(f"Resuming from query {start_index} ({start_index} already collected)")
    else:
        start_index = 0

    for idx, sql in enumerate(sql_statements[start_index:], start=start_index):
        query_with_db = sql["query"]
        query, database = query_with_db.rsplit("@", 1)
        print(f"\n[{idx + 1}/{len(sql_statements)}] Processing query on {database}...")
        print(f"  Query: {query[:80]}...")

        # First: Record metrics by running the actual query (repeat times and average)
        total_cputime, total_scan, total_duration = 0, 0, 0
        for run in range(repeat):
            print(f"  Run {run + 1}/{repeat}")
            _, cputime, scan, duration = record_metrics(
                config["host"], config["databend_port"], config["prometheus_port"],
                query, config["wait"], database
            )
            total_cputime += cputime
            total_scan += scan
            total_duration += duration
            print(f"    Duration: {duration:.3f}s, CPU: {cputime:.2f}, Scan: {scan:.0f}")

        avg_cputime = total_cputime / repeat
        avg_scan = total_scan / repeat
        avg_duration = total_duration / repeat
        print(f"  Averages - CPU: {avg_cputime:.2f}, Scan: {avg_scan:.0f}, Duration: {avg_duration:.3f}s")

        # Second: Get operator info using EXPLAIN ANALYZE
        print("  Getting operator info...")
        operators = record_operator(config["host"], config["databend_port"], query, database)
        print(f"  Operators: {operators}")

        data.append({
            "query": query_with_db,
            "avg_cpu_time": avg_cputime,
            "avg_scan_bytes": avg_scan,
            "avg_duration": avg_duration,
            **operators
        })
        save_data_to_file(data, record_file)

    print(f"\n{'=' * 60}")
    print(f"Done! Collected metrics for {len(data)} {args.benchmark.upper()} queries")
    print(f"Output saved to: {record_file}")


if __name__ == "__main__":
    main()
