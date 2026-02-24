import argparse
import json
import os
import re
import time
from pathlib import Path
from datetime import datetime
import sys

from databend_py import Client
from dotenv import load_dotenv

# Make sure the repo's `src` directory is on the import path so we can reuse utils.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.prometheus import prometheus_queries

# Optional database support
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

# Benchmark configurations
# Each benchmark has:
#   - input: Databend-specific SQL input file
#   - input_postgres: PostgreSQL-specific SQL input file
#   - input_duckdb: DuckDB-specific SQL input file
#   - output: Output file path
BENCHMARKS = {
    "tpch": {
        "input": "./metrics_witho/input/TPCH-tpch1g-sql-input.json",
        "input_postgres": "./metrics_witho/input/TPCH-tpch1g-sql-input-standard.json",
        "input_duckdb": "./metrics_witho/input/TPCH-tpch1g-sql-input-standard.json",
        "output": "./metrics_witho/output/TPCH-tpch1g-sql-metrics.json",
        "description": "TPC-H benchmark (22 queries)"
    },
    "imdb": {
        "input": "./metrics_witho/input/imdb-imdb-sql-input.json",
        "input_postgres": "./metrics_witho/input/imdb-imdb-sql-input-standard.json",
        "input_duckdb": "./metrics_witho/input/imdb-imdb-sql-input-standard.json",
        "output": "./metrics_witho/output/imdb-imdb-sql-metrics.json",
        "description": "IMDB/JOB benchmark (113 queries)"
    },
    "tpcds": {
        "input": "./metrics_witho/input/tpcds_all-tpcds1g-sql-input.json",
        "input_postgres": "./metrics_witho/input/tpcds_all-tpcds1g-sql-input-postgres.json",
        "input_duckdb": "./metrics_witho/input/tpcds_all-tpcds1g-sql-input-duckdb.json",
        "output": "./metrics_witho/output/tpcds_all-tpcds1g-sql-metrics.json",
        "description": "TPC-DS benchmark"
    },
    "ceb": {
        "input":          "./metrics_witho/input/ceb-imdb-sql-input.json",
        "input_postgres": "./metrics_witho/input/ceb-imdb-sql-input-postgres.json",
        "input_duckdb":   "./metrics_witho/input/ceb-imdb-sql-input-duckdb.json",
        "output":         "./metrics_witho/output/ceb-imdb-sql-metrics.json",
        "description": "CEB benchmark"
    },
    "redbench": {
        "input":          "./metrics_witho/input/redbench-imdb-sql-input.json",
        "input_postgres": "./metrics_witho/input/redbench-imdb-sql-input-postgres.json",
        "input_duckdb":   "./metrics_witho/input/redbench-imdb-sql-input-duckdb.json",
        "output":         "./metrics_witho/output/redbench-imdb-sql-metrics.json",
        "description": "Redbench benchmark (JOB + sampled CEB on imdb database)"
    }
}

# PostgreSQL block size (default 8KB)
PG_BLOCK_SIZE = 8192


def get_time():
    """Return a timestamp (seconds since epoch)."""
    return time.time()


def load_config():
    """ Load configuration from environment variable. """
    load_dotenv()
    return {
        "host": os.getenv("HOST", "localhost"),
        "databend_port": os.getenv("DATABEND_PORT", "8000"),
        "prometheus_port": os.getenv("PROMETHEUS_PORT", "9091"),
        # PostgreSQL config
        "pg_host": os.getenv("PG_HOST", ""),  # Empty = local Unix socket (peer auth)
        "pg_port": os.getenv("PG_PORT", "5432"),
        "pg_user": os.getenv("PG_USER", os.getenv("USER", "postgres")),
        "pg_password": os.getenv("PG_PASSWORD", ""),
        "pg_database": os.getenv("PG_DATABASE", "postgres"),
        # DuckDB config
        "duckdb_path": os.getenv("DUCKDB_PATH", ":memory:"),
        # General
        "query": os.getenv("LP_QUERY_SET", "").split(","),
        "db": os.getenv("LP_DATABASE", "").split(","),
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
    sorted_sql_statements = [stmt[1] for stmt in sql_statements]

    return sorted_sql_statements


def load_query_from_json(path):
    with open(path, "r") as file:
        return json.load(file)


# =============================================================================
# Databend Functions
# =============================================================================

def execute_query_databend(host, port, query, database, explain_analyze=False):
    """Execute query on Databend."""
    query_parts = query.split(";")
    if query_parts[-1] == "":
        query_parts = query_parts[:-1]
    query_parts = [q + ";" for q in query_parts]
    ret = []
    for q in query_parts:
        if explain_analyze and not q.upper().startswith("EXPLAIN ANALYZE"):
            q = "EXPLAIN ANALYZE " + q
        client = Client(f"root:@{host}", port=port, secure=False, database=database)
        print(f"    [Databend] Executing: {q[:70]}...")
        try:
            tmp = client.execute(q)
            ret.append(tmp)
        except Exception as e:
            print(f"    [Databend] Error: {e}")
            pass
    return ret


def record_metrics_databend(host, databend_port, prometheus_port, query, database):
    """ Record metrics from Databend using Prometheus. """
    # Wait for fresh Prometheus scrape before starting (scrape interval is 5s)
    time.sleep(6)

    start_time = get_time()
    print(f"  Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    start_cputime = prometheus_queries["cpu"](host, prometheus_port, start_time)
    start_scan = prometheus_queries["scan"](host, prometheus_port, start_time)
    print(f"    Start - CPU: {start_cputime}, Scan: {start_scan}")

    # Run the actual query (not EXPLAIN ANALYZE) to capture real metrics
    query_start = get_time()
    execute_query_databend(host, databend_port, query, database, explain_analyze=False)
    query_duration = get_time() - query_start

    # Wait for Prometheus to scrape new metrics
    time.sleep(6)

    end_time = get_time()
    print(f"  End time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    end_cputime = prometheus_queries["cpu"](host, prometheus_port, end_time)
    end_scan = prometheus_queries["scan"](host, prometheus_port, end_time)
    print(f"    End   - CPU: {end_cputime}, Scan: {end_scan}")

    return end_cputime - start_cputime, end_scan - start_scan, query_duration


def record_operator_databend(host, databend_port, query, database):
    """ Record the operators used in the query (Databend). """
    operator_keywords = {
        "filter": "filters:",
        "join": "HashJoin",
        "agg": "AggregateFinal",
        "sort": "Sort"
    }
    # Use EXPLAIN ANALYZE to get the query plan for operator detection
    plan = execute_query_databend(host, databend_port, query, database, explain_analyze=True)
    operator_flag = {}
    for i in range(len(plan)):
        plan_rows = plan[i][1] if len(plan[i]) > 1 else []
        if not plan_rows:
            print("    Warning: no plan rows returned; operator flags may be incomplete.")
        tmp = '\n'.join([row[0] for row in plan_rows if row])
        for operator, keyword in operator_keywords.items():
            if keyword in tmp:
                operator_flag[operator] = 1
            else:
                operator_flag[operator] = 0
    return operator_flag


# =============================================================================
# PostgreSQL Functions
# =============================================================================

def get_pg_connection(config, database=None):
    """Get PostgreSQL connection with statement timeout from config."""
    if not POSTGRES_AVAILABLE:
        raise RuntimeError("psycopg2 not installed. Run: pip install psycopg2-binary")

    db = database if database else config["pg_database"]
    timeout_ms = config.get("timeout_ms", 60000)  # Default 60s
    # Build connection params - omit host for local Unix socket (peer auth)
    conn_params = {
        "port": config["pg_port"],
        "user": config["pg_user"],
        "database": db,
        "options": f"-c statement_timeout={timeout_ms}"
    }
    if config["pg_host"]:
        conn_params["host"] = config["pg_host"]
    if config["pg_password"]:
        conn_params["password"] = config["pg_password"]

    return psycopg2.connect(**conn_params)


def execute_query_postgres(config, query, database):
    """Execute query on PostgreSQL and return results."""
    conn = get_pg_connection(config, database)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:
                return cur.fetchall()
            return []
    finally:
        conn.close()


def record_metrics_postgres(config, query, database):
    """
    Record metrics from PostgreSQL using EXPLAIN (ANALYZE, BUFFERS).
    Returns: (cpu_time_ms, data_scanned_bytes, duration_s)

    PostgreSQL doesn't expose CPU time directly, but we use:
    - Execution time from EXPLAIN ANALYZE as a proxy
    - Buffer stats (shared_blks_read + shared_blks_hit) * block_size for data scanned
    """
    conn = get_pg_connection(config, database)
    try:
        with conn.cursor() as cur:
            # Use EXPLAIN with ANALYZE and BUFFERS to get timing and I/O stats
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"

            start_time = get_time()
            cur.execute(explain_query)
            duration = get_time() - start_time

            result = cur.fetchone()[0]
            plan = result[0] if result else {}

            # Extract execution time (in ms)
            execution_time_ms = plan.get("Execution Time", 0)
            planning_time_ms = plan.get("Planning Time", 0)
            total_time_ms = execution_time_ms + planning_time_ms

            # Extract buffer stats from the plan
            # We need to recursively sum all buffer usage
            def extract_buffers(node):
                buffers = {
                    "shared_hit": node.get("Shared Hit Blocks", 0),
                    "shared_read": node.get("Shared Read Blocks", 0),
                    "local_hit": node.get("Local Hit Blocks", 0),
                    "local_read": node.get("Local Read Blocks", 0),
                    "temp_read": node.get("Temp Read Blocks", 0),
                    "temp_written": node.get("Temp Written Blocks", 0),
                }
                # Recurse into child plans
                for child in node.get("Plans", []):
                    child_buffers = extract_buffers(child)
                    for k in buffers:
                        buffers[k] += child_buffers[k]
                return buffers

            root_plan = plan.get("Plan", {})
            buffers = extract_buffers(root_plan)

            # Total blocks accessed (read from disk + hit in cache)
            total_blocks = (buffers["shared_hit"] + buffers["shared_read"] +
                          buffers["local_hit"] + buffers["local_read"])
            data_scanned_bytes = total_blocks * PG_BLOCK_SIZE

            print(f"    [PostgreSQL] Exec: {execution_time_ms:.2f}ms, "
                  f"Blocks: {total_blocks} ({data_scanned_bytes/1024/1024:.2f}MB)")

            return total_time_ms, data_scanned_bytes, duration

    except Exception as e:
        print(f"    [PostgreSQL] Error: {e}")
        return 0, 0, 0
    finally:
        conn.close()


def record_operator_postgres(config, query, database):
    """Record the operators used in the query (PostgreSQL)."""
    operator_keywords = {
        "filter": "Filter",
        "join": "Join",  # Covers Hash Join, Nested Loop, Merge Join
        "agg": "Aggregate",
        "sort": "Sort"
    }

    conn = get_pg_connection(config, database)
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT TEXT) {query}")
            plan_text = "\n".join([row[0] for row in cur.fetchall()])

            operator_flag = {}
            for operator, keyword in operator_keywords.items():
                operator_flag[operator] = 1 if keyword in plan_text else 0
            return operator_flag
    except Exception as e:
        print(f"    [PostgreSQL] Error getting operators: {e}")
        return {"filter": 0, "join": 0, "agg": 0, "sort": 0}
    finally:
        conn.close()


# =============================================================================
# DuckDB Functions
# =============================================================================

def get_duckdb_connection(config, database=None):
    """Get DuckDB connection."""
    if not DUCKDB_AVAILABLE:
        raise RuntimeError("duckdb not installed. Run: pip install duckdb")

    # For DuckDB, database is typically a file path or :memory:
    db_path = database if database else config["duckdb_path"]
    return duckdb.connect(db_path)


def execute_query_duckdb(config, query, database):
    """Execute query on DuckDB and return results."""
    conn = get_duckdb_connection(config, database)
    try:
        return conn.execute(query).fetchall()
    finally:
        conn.close()


def record_metrics_duckdb(config, query, database):
    """
    Record metrics from DuckDB using EXPLAIN ANALYZE.
    Returns: (cpu_time_ms, data_scanned_bytes, duration_s)

    DuckDB provides detailed profiling through EXPLAIN ANALYZE which includes:
    - Timing per operator
    - Cardinality (rows processed)

    For data scanned, we estimate based on tables accessed and their sizes.
    """
    conn = get_duckdb_connection(config, database)
    try:
        # First, get table sizes for estimating data scanned
        table_sizes = {}
        try:
            # Query table storage info
            size_result = conn.execute("""
                SELECT table_name, estimated_size
                FROM duckdb_tables()
                WHERE estimated_size IS NOT NULL
            """).fetchall()
            for row in size_result:
                table_sizes[row[0].lower()] = row[1]
        except Exception:
            pass  # Table size info not available

        # Run EXPLAIN ANALYZE to get detailed metrics
        try:
            explain_result = conn.execute(f"EXPLAIN ANALYZE {query}").fetchall()
            explain_text = "\n".join([str(row) for row in explain_result])

            # Parse the cumulative timing from the output
            # DuckDB EXPLAIN ANALYZE shows timing like: "│       0.02 │" for each operator
            # The root node typically has the total time
            cpu_time_ms = 0.0

            # Look for timing patterns in the output (format: │ time │)
            # DuckDB shows time in seconds with decimal
            time_matches = re.findall(r'│\s*(\d+\.?\d*)\s*│', explain_text)
            if time_matches:
                # The first timing value is usually the total
                try:
                    # Convert to ms (DuckDB shows seconds)
                    cpu_time_ms = float(time_matches[0]) * 1000
                except (ValueError, IndexError):
                    pass

            # Fallback: look for explicit timing in different format
            if cpu_time_ms == 0:
                time_match = re.search(r'(\d+\.?\d*)\s*(?:ms|milliseconds|s|seconds)', explain_text, re.IGNORECASE)
                if time_match:
                    val = float(time_match.group(1))
                    if 'ms' in time_match.group(0).lower() or 'milliseconds' in time_match.group(0).lower():
                        cpu_time_ms = val
                    else:
                        cpu_time_ms = val * 1000

            # Estimate data scanned from tables referenced in the query
            data_scanned_bytes = 0
            # Simple heuristic: find table names in query and sum their sizes
            query_lower = query.lower()
            for table_name, size in table_sizes.items():
                # Check if table is referenced in the query
                if re.search(rf'\b{re.escape(table_name)}\b', query_lower):
                    data_scanned_bytes += size

            # If no size info, try to estimate from row counts in EXPLAIN
            if data_scanned_bytes == 0:
                # Look for row counts in the output
                row_matches = re.findall(r'(\d+)\s*(?:rows?|tuples?)', explain_text, re.IGNORECASE)
                if row_matches:
                    # Very rough estimate: assume 100 bytes per row average
                    total_rows = sum(int(r) for r in row_matches[:5])  # Cap to avoid huge overestimates
                    data_scanned_bytes = total_rows * 100

            # Also measure wall clock time for comparison
            start_time = get_time()
            conn.execute(query).fetchall()
            duration = get_time() - start_time

            # If no CPU time extracted, use wall clock
            if cpu_time_ms == 0:
                cpu_time_ms = duration * 1000

            print(f"    [DuckDB] CPU: {cpu_time_ms:.2f}ms, Scan: {data_scanned_bytes/1024/1024:.2f}MB, Wall: {duration*1000:.2f}ms")

            return cpu_time_ms, data_scanned_bytes, duration

        except Exception as e:
            print(f"    [DuckDB] EXPLAIN ANALYZE failed: {e}")
            # Fallback: just measure execution time
            start_time = get_time()
            conn.execute(query).fetchall()
            duration = get_time() - start_time
            return duration * 1000, 0, duration

    except Exception as e:
        print(f"    [DuckDB] Error: {e}")
        return 0, 0, 0
    finally:
        conn.close()


def record_operator_duckdb(config, query, database):
    """Record the operators used in the query (DuckDB)."""
    operator_keywords = {
        "filter": "FILTER",
        "join": "JOIN",  # Covers HASH_JOIN, etc.
        "agg": "AGGREGATE",
        "sort": "ORDER"
    }

    conn = get_duckdb_connection(config, database)
    try:
        result = conn.execute(f"EXPLAIN {query}").fetchall()
        plan_text = "\n".join([str(row) for row in result]).upper()

        operator_flag = {}
        for operator, keyword in operator_keywords.items():
            operator_flag[operator] = 1 if keyword in plan_text else 0
        return operator_flag
    except Exception as e:
        print(f"    [DuckDB] Error getting operators: {e}")
        return {"filter": 0, "join": 0, "agg": 0, "sort": 0}
    finally:
        conn.close()


# =============================================================================
# Main
# =============================================================================

def save_data_to_file(data, record_file):
    """ Save data to a file. """
    with open(record_file, "w") as file:
        json.dump(data, file, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Collect metrics for database benchmark queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available benchmarks:
  tpch   - TPC-H benchmark (22 queries on tpch1g database)
  imdb   - IMDB/JOB benchmark (113 queries on imdb database)
  tpcds  - TPC-DS benchmark (on tpcds1g database)
  ceb    - CEB benchmark (13646 on imdb database)
  redbench - Redbench benchmark (JOB + sampled CEB on imdb database)

Database backends:
  --databend     Collect from Databend (default, uses Prometheus)
  --postgres     Collect from PostgreSQL (uses EXPLAIN ANALYZE BUFFERS)
  --duckdb       Collect from DuckDB (uses EXPLAIN ANALYZE)

Examples:
  # Databend only (default)
  python collect.py tpch

  # PostgreSQL only
  python collect.py tpcds --no-databend --postgres --pg-database tpcds1g

  # DuckDB only
  python collect.py tpcds --no-databend --duckdb --duckdb-path ./tpcds1g.duckdb

  # All three databases
  python collect.py tpch --all --pg-database tpch1g --duckdb-path ./tpch1g.duckdb
        """
    )
    parser.add_argument(
        "benchmark",
        choices=list(BENCHMARKS.keys()),
        help="Benchmark to run (tpch, imdb, tpcds, ceb, or redbench)"
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
    parser.add_argument(
        "--databend",
        action="store_true",
        default=True,
        help="Collect metrics from Databend (default: enabled)"
    )
    parser.add_argument(
        "--no-databend",
        action="store_true",
        help="Disable Databend collection"
    )
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="Also collect metrics from PostgreSQL"
    )
    parser.add_argument(
        "--duckdb",
        action="store_true",
        help="Also collect metrics from DuckDB"
    )
    parser.add_argument(
        "--pg-database",
        type=str,
        default=None,
        help="PostgreSQL database name (overrides PG_DATABASE env var)"
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default=None,
        help="DuckDB database path (overrides DUCKDB_PATH env var)"
    )
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=60,
        help="Query timeout in seconds (default: 60). Increase for slow queries."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Collect from all three databases (Databend, PostgreSQL, DuckDB)"
    )
    args = parser.parse_args()

    # Check dependencies
    if args.postgres and not POSTGRES_AVAILABLE:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)
    if args.duckdb and not DUCKDB_AVAILABLE:
        print("ERROR: duckdb not installed. Run: pip install duckdb")
        sys.exit(1)

    config = load_config()

    # Handle --all flag
    if args.all:
        args.postgres = True
        args.duckdb = True

    # Override config from CLI args
    if args.pg_database:
        config["pg_database"] = args.pg_database
    if args.duckdb_path:
        config["duckdb_path"] = args.duckdb_path
    config["timeout_ms"] = args.timeout * 1000  # Convert seconds to ms

    # If only --postgres or --duckdb is specified (without explicit --databend), disable Databend
    # This allows "python collect.py tpch --duckdb" to run DuckDB only
    explicit_databend = "--databend" in sys.argv
    use_databend = (args.databend and not args.no_databend and
                    (explicit_databend or (not args.postgres and not args.duckdb)))

    benchmark = BENCHMARKS[args.benchmark]
    src_databend = benchmark["input"]
    src_postgres = benchmark.get("input_postgres", benchmark["input"])
    src_duckdb = benchmark.get("input_duckdb", benchmark["input"])
    record_file = benchmark["output"]
    repeat = args.repeat

    # Determine output file names for each database
    base_output = record_file.replace(".json", "")
    output_files = {}
    if use_databend:
        output_files["databend"] = record_file
    if args.postgres:
        output_files["postgres"] = f"{base_output}-postgres.json"
    if args.duckdb:
        output_files["duckdb"] = f"{base_output}-duckdb.json"

    print(f"Benchmark: {args.benchmark.upper()} - {benchmark['description']}")
    print(f"Databases: {', '.join(output_files.keys())}")
    print(f"Repeat: {repeat}x per query")
    print(f"Timeout: {args.timeout}s per query")
    print("=" * 60)

    # Load queries - different input files for different databases
    sql_statements_databend = None
    sql_statements_postgres = None
    sql_statements_duckdb = None

    if use_databend:
        print(f"\nLoading Databend queries from: {src_databend}")
        sql_statements_databend = load_query_from_json(src_databend)
        print(f"Found {len(sql_statements_databend)} queries for Databend")

    if args.postgres:
        print(f"\nLoading PostgreSQL queries from: {src_postgres}")
        if os.path.exists(src_postgres):
            sql_statements_postgres = load_query_from_json(src_postgres)
            print(f"Found {len(sql_statements_postgres)} queries for PostgreSQL")
        else:
            print(f"  WARNING: PostgreSQL input file not found: {src_postgres}")
            sql_statements_postgres = load_query_from_json(src_databend)

    if args.duckdb:
        print(f"\nLoading DuckDB queries from: {src_duckdb}")
        if os.path.exists(src_duckdb):
            sql_statements_duckdb = load_query_from_json(src_duckdb)
            print(f"Found {len(sql_statements_duckdb)} queries for DuckDB")
        else:
            print(f"  WARNING: DuckDB input file not found: {src_duckdb}")
            print(f"  Falling back to Databend input (queries may fail)")
            sql_statements_duckdb = load_query_from_json(src_databend)

    # Use Databend queries as the iteration base, or postgres/duckdb if no Databend
    sql_statements = (sql_statements_databend or sql_statements_postgres or
                      sql_statements_duckdb)
    num_queries = len(sql_statements)

    # Initialize data storage for each database
    data = {db: [] for db in output_files}

    # Resume from existing progress
    if args.start is not None:
        start_index = args.start
        print(f"Starting from query index {start_index} (as specified)")
    else:
        # Load existing data for all enabled databases
        start_index = 0
        for db_name, output_path in output_files.items():
            if os.path.exists(output_path):
                with open(output_path, "r") as file:
                    data[db_name] = json.load(file)

        # Resume from minimum progress across enabled databases
        # This ensures we don't skip queries for databases that are behind
        progress_counts = [len(data[db]) for db in output_files]
        if progress_counts:
            start_index = min(progress_counts)
            if start_index > 0:
                print(f"Resuming from query {start_index} ({start_index} already collected)")

    for idx in range(start_index, num_queries):
        # Get query for each database type
        databend_query = None
        databend_query_with_db = None
        postgres_query = None
        postgres_query_with_db = None
        duckdb_query = None
        duckdb_query_with_db = None
        database = "unknown"

        if sql_statements_databend:
            databend_sql = sql_statements_databend[idx]
            databend_query_with_db = databend_sql["query"]
            databend_query, database = databend_query_with_db.rsplit("@", 1)

        if sql_statements_postgres:
            postgres_sql = sql_statements_postgres[idx]
            postgres_query_with_db = postgres_sql["query"]
            postgres_query, db = postgres_query_with_db.rsplit("@", 1)
            if database == "unknown":
                database = db

        if sql_statements_duckdb:
            duckdb_sql = sql_statements_duckdb[idx]
            duckdb_query_with_db = duckdb_sql["query"]
            duckdb_query, db = duckdb_query_with_db.rsplit("@", 1)
            if database == "unknown":
                database = db

        # For display, prefer databend query, fall back to postgres/duckdb
        display_query = databend_query or postgres_query or duckdb_query
        print(f"\n[{idx + 1}/{num_queries}] Processing query on {database}...")
        print(f"  Query: {display_query[:80] if display_query else 'N/A'}...")

        # ---- Databend ----
        if use_databend and databend_query:
            print("  [Databend]")
            total_cputime, total_scan, total_duration = 0, 0, 0
            for run in range(repeat):
                print(f"    Run {run + 1}/{repeat}")
                cputime, scan, duration = record_metrics_databend(
                    config["host"], config["databend_port"], config["prometheus_port"],
                    databend_query, database
                )
                total_cputime += cputime
                total_scan += scan
                total_duration += duration
                print(f"      Duration: {duration:.3f}s, CPU: {cputime:.2f}, Scan: {scan:.0f}")

            avg_cputime = total_cputime / repeat
            avg_scan = total_scan / repeat
            avg_duration = total_duration / repeat

            operators = record_operator_databend(config["host"], config["databend_port"], databend_query, database)

            data["databend"].append({
                "query": databend_query_with_db,
                "avg_cpu_time": avg_cputime,
                "avg_scan_bytes": avg_scan,
                "avg_duration": avg_duration,
                **operators
            })
            save_data_to_file(data["databend"], output_files["databend"])

        # ---- PostgreSQL ----
        if args.postgres and postgres_query:
            print("  [PostgreSQL]")
            pg_db = args.pg_database if args.pg_database else database
            total_cputime, total_scan, total_duration = 0, 0, 0
            for run in range(repeat):
                print(f"    Run {run + 1}/{repeat}")
                cputime, scan, duration = record_metrics_postgres(config, postgres_query, pg_db)
                total_cputime += cputime
                total_scan += scan
                total_duration += duration

            avg_cputime = total_cputime / repeat
            avg_scan = total_scan / repeat
            avg_duration = total_duration / repeat

            operators = record_operator_postgres(config, postgres_query, pg_db)

            data["postgres"].append({
                "query": postgres_query_with_db,
                "avg_cpu_time": avg_cputime,
                "avg_scan_bytes": avg_scan,
                "avg_duration": avg_duration,
                **operators
            })
            save_data_to_file(data["postgres"], output_files["postgres"])

        # ---- DuckDB ----
        if args.duckdb and duckdb_query:
            print("  [DuckDB]")
            duck_db = args.duckdb_path if args.duckdb_path else config["duckdb_path"]
            total_cputime, total_scan, total_duration = 0, 0, 0
            for run in range(repeat):
                print(f"    Run {run + 1}/{repeat}")
                cputime, scan, duration = record_metrics_duckdb(config, duckdb_query, duck_db)
                total_cputime += cputime
                total_scan += scan
                total_duration += duration

            avg_cputime = total_cputime / repeat
            avg_scan = total_scan / repeat
            avg_duration = total_duration / repeat

            operators = record_operator_duckdb(config, duckdb_query, duck_db)

            data["duckdb"].append({
                "query": duckdb_query_with_db,
                "avg_cpu_time": avg_cputime,
                "avg_scan_bytes": avg_scan,
                "avg_duration": avg_duration,
                **operators
            })
            save_data_to_file(data["duckdb"], output_files["duckdb"])

    print(f"\n{'=' * 60}")
    print(f"Done! Collected metrics for {args.benchmark.upper()} queries")
    for db_name, output_path in output_files.items():
        print(f"  {db_name}: {output_path} ({len(data[db_name])} queries)")


if __name__ == "__main__":
    main()
