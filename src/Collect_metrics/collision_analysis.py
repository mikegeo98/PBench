#!/usr/bin/env python3
"""Generate CSV of telemetry collision pairs across TPC-H and TPC-DS SF10."""

import json
import csv
import re
import sys
import math
from itertools import combinations

TPCH_FILE = "./metrics_witho/output/TPCH-tpch10g-sql-metrics.json"
TPCDS_FILE = "./metrics_witho/output/tpcds_all-tpcds10g-sql-metrics.json"
OUTPUT_CSV = "./collision_pairs_sf10.csv"

# --- SQL parsing helpers ---

TABLE_ALIASES = {
    "l1", "l2", "l3", "n1", "n2", "t1", "t2", "t3",
    "shipping", "all_nations", "profit", "c_orders", "custsale",
    "revenue", "part_agg", "year_total",
}

TPCH_TABLES = {
    "lineitem", "orders", "customer", "supplier", "partsupp",
    "part", "nation", "region",
}

TPCDS_TABLES = {
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory", "customer", "customer_address",
    "customer_demographics", "date_dim", "warehouse", "ship_mode",
    "time_dim", "reason", "income_band", "item", "store", "call_center",
    "web_page", "web_site", "catalog_page", "household_demographics",
    "promotion",
}

ALL_TABLES = TPCH_TABLES | TPCDS_TABLES


def extract_tables(sql):
    """Extract table names from SQL."""
    tables = set()
    # FROM and JOIN clauses
    for m in re.finditer(r'\b(?:FROM|JOIN)\s+(\w+)', sql, re.IGNORECASE):
        name = m.group(1).lower()
        if name in ALL_TABLES:
            tables.add(name)
    # Comma-separated tables in FROM
    from_blocks = re.findall(r'FROM\s+((?:\w+\s*(?:\w+)?\s*,\s*)*\w+\s*(?:\w+)?)', sql, re.IGNORECASE)
    for block in from_blocks:
        for part in block.split(','):
            token = part.strip().split()[0].lower() if part.strip() else ""
            if token in ALL_TABLES:
                tables.add(token)
    return tables


def count_joins(sql):
    """Count JOIN keywords."""
    explicit = len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))
    # Count implicit joins from comma-separated tables in FROM
    tables_in_from = []
    for block in re.findall(r'FROM\s+((?:[^()]*?))\s*(?:WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|EXCEPT|INTERSECT|;|$)', sql, re.IGNORECASE):
        parts = [p.strip() for p in block.split(',') if p.strip()]
        valid = [p for p in parts if p.split()[0].lower() in ALL_TABLES or p.split()[0].lower() in TABLE_ALIASES]
        if len(valid) > 1:
            tables_in_from.append(len(valid) - 1)
    implicit = sum(tables_in_from)
    return explicit + implicit


def count_subqueries(sql):
    """Count subqueries (SELECT inside parentheses, minus CTEs)."""
    # Count all SELECT keywords minus the outermost one
    selects = len(re.findall(r'\bSELECT\b', sql, re.IGNORECASE))
    ctes = len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))
    return max(0, selects - 1 - ctes)


def count_ctes(sql):
    # Count CTE definitions (AS ( before SELECT in WITH clause)
    return len(re.findall(r'\bWITH\b', sql, re.IGNORECASE))


def has_window(sql):
    return 1 if re.search(r'\bOVER\s*\(', sql, re.IGNORECASE) else 0


def has_union(sql):
    return 1 if re.search(r'\bUNION\b', sql, re.IGNORECASE) else 0


def query_label(query_text):
    """Generate a short label from the query."""
    # Extract database suffix
    db = ""
    if "@" in query_text:
        db = query_text.split("@")[-1].strip()

    # Try to identify TPC-H queries by their structure
    sql = query_text.split("@")[0] if "@" in query_text else query_text
    sql_lower = sql.lower().strip()

    if "tpch" in db:
        # Map by known patterns
        if "l_returnflag" in sql_lower and "l_linestatus" in sql_lower and "sum_qty" in sql_lower:
            return "TPCH-Q1"
        if "s_acctbal" in sql_lower and "p_mfgr" in sql_lower and "min(ps_supplycost)" in sql_lower:
            return "TPCH-Q2"
        if "o_shippriority" in sql_lower and "c_mktsegment" in sql_lower and "building" in sql_lower:
            return "TPCH-Q3"
        if "o_orderpriority" in sql_lower and "order_count" in sql_lower and "l_commitdate < l_receiptdate" in sql_lower:
            return "TPCH-Q4"
        if "n_name" in sql_lower and "r_name = 'asia'" in sql_lower and "revenue" in sql_lower:
            return "TPCH-Q5"
        if "l_discount" in sql_lower and "l_quantity < 24" in sql_lower and sql_lower.count("from") == 1:
            return "TPCH-Q6"
        if "supp_nation" in sql_lower and "cust_nation" in sql_lower:
            return "TPCH-Q7"
        if "mkt_share" in sql_lower and "all_nations" in sql_lower:
            return "TPCH-Q8"
        if "sum_profit" in sql_lower and "profit" in sql_lower:
            return "TPCH-Q9"
        if "c_acctbal" in sql_lower and "l_returnflag = 'r'" in sql_lower:
            return "TPCH-Q10"
        if "ps_partkey" in sql_lower and "ps_supplycost * ps_availqty" in sql_lower:
            return "TPCH-Q11"
        if "l_shipmode" in sql_lower and "high_line_count" in sql_lower:
            return "TPCH-Q12"
        if "c_count" in sql_lower and "custdist" in sql_lower:
            return "TPCH-Q13"
        if "promo_revenue" in sql_lower:
            return "TPCH-Q14"
        if "total_revenue" in sql_lower and "revenue" in sql_lower and "with" in sql_lower:
            return "TPCH-Q15"
        if "supplier_cnt" in sql_lower and "p_brand" in sql_lower:
            return "TPCH-Q16"
        if "avg_yearly" in sql_lower and "0.2 * avg(l_quantity)" in sql_lower:
            return "TPCH-Q17"
        if "sum(l_quantity) > 300" in sql_lower:
            return "TPCH-Q18"
        if "deliver in person" in sql_lower:
            return "TPCH-Q19"
        if "forest" in sql_lower and "canada" in sql_lower:
            return "TPCH-Q20"
        if "numwait" in sql_lower and "saudi arabia" in sql_lower:
            return "TPCH-Q21"
        if "cntrycode" in sql_lower and "numcust" in sql_lower:
            return "TPCH-Q22"

    if "tpcds" in db:
        # For TPC-DS we can't easily identify query numbers, use a hash approach
        # Just number them sequentially
        return None  # will be set externally

    return None


def load_metrics(filepath, prefix):
    """Load metrics file and return list of query dicts."""
    with open(filepath) as f:
        data = json.load(f)

    queries = []
    idx = 0
    for entry in data:
        cpu = entry.get("avg_cpu_time", 0.0)
        scan = entry.get("avg_scan_bytes", 0.0)
        dur = entry.get("avg_duration", 0.0)
        sql = entry.get("query", "")

        # Skip failed queries (all zeros)
        if cpu == 0.0 and scan == 0.0 and dur == 0.0:
            idx += 1
            continue

        tables = extract_tables(sql)
        joins = count_joins(sql)
        subq = count_subqueries(sql)

        label = query_label(sql)
        if label is None:
            label = f"{prefix}-Q{idx+1}"

        queries.append({
            "label": label,
            "cpu": cpu,
            "scan": scan,
            "duration": dur,
            "tables": len(tables),
            "joins": joins,
            "subqueries": subq,
            "filter": entry.get("filter", 0),
            "join_op": entry.get("join", 0),
            "agg": entry.get("agg", 0),
            "sort": entry.get("sort", 0),
            "table_set": tables,
            "sql": sql,
        })
        idx += 1

    return queries


def telemetry_distance(a, b, ranges):
    """Weighted range-normalized distance on CPU, scan, duration, operator signature."""
    cpu_d = abs(a["cpu"] - b["cpu"]) / ranges["cpu"]
    scan_d = abs(a["scan"] - b["scan"]) / ranges["scan"]
    dur_d = abs(a["duration"] - b["duration"]) / ranges["duration"]

    # Operator signature distance (binary match on filter, join, agg, sort)
    op_match = sum(1 for k in ["filter", "join_op", "agg", "sort"] if a[k] == b[k])
    op_d = 1.0 - op_match / 4.0

    # Weighted: CPU 30%, scan 30%, duration 25%, operators 15%
    return math.sqrt(0.30 * cpu_d**2 + 0.30 * scan_d**2 + 0.25 * dur_d**2 + 0.15 * op_d**2)


def structural_distance(a, b):
    """How structurally different two queries are."""
    table_diff = abs(a["tables"] - b["tables"])
    join_diff = abs(a["joins"] - b["joins"])
    subq_diff = abs(a["subqueries"] - b["subqueries"])

    # Jaccard distance on table sets
    intersection = len(a["table_set"] & b["table_set"])
    union = len(a["table_set"] | b["table_set"])
    jaccard = 1.0 - (intersection / union if union > 0 else 0)

    return table_diff * 3 + join_diff * 2 + subq_diff * 4 + jaccard * 5


def main():
    tpch = load_metrics(TPCH_FILE, "TPCH")
    tpcds = load_metrics(TPCDS_FILE, "TPCDS")

    all_queries = tpch + tpcds
    print(f"Loaded {len(tpch)} TPC-H + {len(tpcds)} TPC-DS = {len(all_queries)} queries")

    # Compute ranges for normalization
    cpus = [q["cpu"] for q in all_queries]
    scans = [q["scan"] for q in all_queries]
    durs = [q["duration"] for q in all_queries]
    ranges = {
        "cpu": max(cpus) - min(cpus) if max(cpus) != min(cpus) else 1.0,
        "scan": max(scans) - min(scans) if max(scans) != min(scans) else 1.0,
        "duration": max(durs) - min(durs) if max(durs) != min(durs) else 1.0,
    }
    print(f"Ranges — CPU: {min(cpus):.1f}-{max(cpus):.1f}, Scan: {min(scans)/1e6:.1f}-{max(scans)/1e6:.1f}MB, Dur: {min(durs):.2f}-{max(durs):.2f}s")

    # Find collision pairs
    TELE_THRESHOLD = 0.03  # 3%
    MIN_SCAN_BYTES = 3 * 1024 * 1024  # 3 MB

    pairs = []
    for a, b in combinations(all_queries, 2):
        # Filter: both must scan >= 3MB
        if a["scan"] < MIN_SCAN_BYTES or b["scan"] < MIN_SCAN_BYTES:
            continue
        # Filter: both must have CPU > 0
        if a["cpu"] <= 0 or b["cpu"] <= 0:
            continue

        td = telemetry_distance(a, b, ranges)
        if td < TELE_THRESHOLD:
            sd = structural_distance(a, b)
            if sd > 0:  # only pairs that are structurally different
                danger = sd / max(td, 1e-9)
                pairs.append((a, b, td, sd, danger))

    # Sort by danger ratio descending
    pairs.sort(key=lambda x: x[4], reverse=True)
    print(f"Found {len(pairs)} collision pairs (tele < 3%, scan >= 3MB, cpu > 0)")

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Query_A", "CPU_A", "Scan_A_MB", "Duration_A_s", "Tables_A", "Joins_A", "Subqueries_A",
            "Query_B", "CPU_B", "Scan_B_MB", "Duration_B_s", "Tables_B", "Joins_B", "Subqueries_B",
            "Telemetry_Distance", "Structural_Distance", "Danger_Ratio",
        ])
        for a, b, td, sd, danger in pairs:
            writer.writerow([
                a["label"],
                f"{a['cpu']:.1f}",
                f"{a['scan']/1e6:.1f}",
                f"{a['duration']:.2f}",
                a["tables"],
                a["joins"],
                a["subqueries"],
                b["label"],
                f"{b['cpu']:.1f}",
                f"{b['scan']/1e6:.1f}",
                f"{b['duration']:.2f}",
                b["tables"],
                b["joins"],
                b["subqueries"],
                f"{td:.4f}",
                f"{sd:.1f}",
                f"{danger:.1f}",
            ])

    print(f"CSV written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
