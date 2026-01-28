#!/usr/bin/env python3
"""
Convert TPC-H, TPC-DS, and JOB queries to Databend-compatible format
and generate input JSON files for metrics collection.
"""
import json
import os
import re
from pathlib import Path

# TPC-H parameter substitutions (default values from TPC-H spec)
TPCH_PARAMS = {
    '1': {  # Q1: Pricing Summary Report
        '$1': '-90',  # DELTA days
    },
    '2': {  # Q2: Minimum Cost Supplier
        '$1': '15',      # SIZE
        '$2': "'BRASS'",  # TYPE
        '$3': "'EUROPE'", # REGION
    },
    '3': {  # Q3: Shipping Priority
        '$1': "'BUILDING'",  # SEGMENT
        '$2': "'1995-03-15'",  # DATE
    },
    '4': {  # Q4: Order Priority Checking
        '$1': "'1993-07-01'",  # DATE
    },
    '5': {  # Q5: Local Supplier Volume
        '$1': "'ASIA'",        # REGION
        '$2': "'1994-01-01'",  # DATE
    },
    '6': {  # Q6: Forecasting Revenue Change
        '$1': "'1994-01-01'",  # DATE
        '$2': '0.06',          # DISCOUNT
        '$3': '24',            # QUANTITY
    },
    '7': {  # Q7: Volume Shipping
        '$1': "'FRANCE'",  # NATION1
        '$2': "'GERMANY'", # NATION2
    },
    '8': {  # Q8: National Market Share
        '$1': "'BRAZIL'",           # NATION
        '$2': "'AMERICA'",          # REGION
        '$3': "'ECONOMY ANODIZED STEEL'",  # TYPE
    },
    '9': {  # Q9: Product Type Profit Measure
        '$1': "'%green%'",  # COLOR (LIKE pattern)
    },
    '10': {  # Q10: Returned Item Reporting
        '$1': "'1993-10-01'",  # DATE
    },
    '11': {  # Q11: Important Stock Identification
        '$1': "'GERMANY'",  # NATION
        '$2': '0.0001',     # FRACTION
    },
    '12': {  # Q12: Shipping Modes and Order Priority
        '$1': "'MAIL'",        # SHIPMODE1
        '$2': "'SHIP'",        # SHIPMODE2
        '$3': "'1994-01-01'",  # DATE
    },
    '13': {  # Q13: Customer Distribution
        '$1': "'%special%requests%'",  # WORD1 WORD2 (LIKE pattern)
    },
    '14': {  # Q14: Promotion Effect
        '$1': "'1995-09-01'",  # DATE
    },
    '15': {  # Q15: Top Supplier
        '$1': "'1996-01-01'",  # DATE
    },
    '16': {  # Q16: Parts/Supplier Relationship
        '$1': "'Brand#45'",  # BRAND
        '$2': "'MEDIUM POLISHED%'",  # TYPE
        '$3': '49',  # SIZE1
        '$4': '14',  # SIZE2
        '$5': '23',  # SIZE3
        '$6': '45',  # SIZE4
        '$7': '19',  # SIZE5
        '$8': '3',   # SIZE6
        '$9': '36',  # SIZE7
        '$10': '9',  # SIZE8
    },
    '17': {  # Q17: Small-Quantity-Order Revenue
        '$1': "'Brand#23'",     # BRAND
        '$2': "'MED BOX'",      # CONTAINER
    },
    '18': {  # Q18: Large Volume Customer
        '$1': '300',  # QUANTITY
    },
    '19': {  # Q19: Discounted Revenue
        '$1': "'Brand#12'",  # BRAND1
        '$2': "'Brand#23'",  # BRAND2
        '$3': "'Brand#34'",  # BRAND3
        '$4': '1',   # QUANTITY1
        '$5': '10',  # QUANTITY2
        '$6': '20',  # QUANTITY3
    },
    '20': {  # Q20: Potential Part Promotion
        '$1': "'forest%'",     # COLOR
        '$2': "'1994-01-01'",  # DATE
        '$3': "'CANADA'",      # NATION
    },
    '21': {  # Q21: Suppliers Who Kept Orders Waiting
        '$1': "'SAUDI ARABIA'",  # NATION
    },
    '22': {  # Q22: Global Sales Opportunity
        '$1': "'13'",  # I1
        '$2': "'31'",  # I2
        '$3': "'23'",  # I3
        '$4': "'29'",  # I4
        '$5': "'30'",  # I5
        '$6': "'18'",  # I6
        '$7': "'17'",  # I7
    },
    '23': {  # Non-standard (if exists)
    },
}


def convert_tpch_query(sql: str, query_num: str) -> str:
    """Convert TPC-H query from Redshift format to Databend format."""
    # Remove table placeholders (e.g., :lineitem -> lineitem)
    sql = re.sub(r':(\w+)', r'\1', sql)

    # Replace parameters with actual values
    params = TPCH_PARAMS.get(query_num, {})
    for param, value in params.items():
        sql = sql.replace(param, str(value))

    # Convert DATEADD(day, N, 'date') to DATE_ADD('date', INTERVAL N DAY)
    # Pattern: DATEADD(day, -90, '1998-12-01')
    sql = re.sub(
        r"DATEADD\s*\(\s*day\s*,\s*(-?\d+)\s*,\s*'([^']+)'\s*\)",
        r"DATE_ADD('\2', INTERVAL \1 DAY)",
        sql,
        flags=re.IGNORECASE
    )

    # Fix "desc limit" -> "LIMIT" (remove extra desc)
    sql = re.sub(r'\bdesc\s+limit\b', 'LIMIT', sql, flags=re.IGNORECASE)

    return sql.strip()


def convert_tpcds_query(sql: str) -> str:
    """Convert TPC-DS query to Databend format (minimal changes needed)."""
    # Remove comment lines at the start
    lines = sql.strip().split('\n')
    lines = [l for l in lines if not l.strip().startswith('--')]
    return '\n'.join(lines).strip()


def convert_job_query(sql: str) -> str:
    """Convert JOB query to Databend format (minimal changes needed)."""
    return sql.strip()


def flatten_sql(sql: str) -> str:
    """Return a single-line SQL string with normalized spacing."""
    return " ".join(sql.strip().split())


def convert_ceb_query(sql: str, dialect: str) -> str:
    """
    Convert CEB query to the requested dialect.

    At the moment the queries are written in a PostgreSQL-compatible style
    (ILIKE, ::float casts, regex '~'). Apply dialect-specific tweaks where
    needed before flattening.
    """
    sql = sql.strip()

    # Regex operator: replace "~ 'pattern'" with the dialect's regex form.
    if dialect in ("duckdb", "databend"):
        sql = re.sub(r"(\S+)\s*~\s*'([^']*)'", r"\1 REGEXP '\2'", sql)

    # Databend does not support ILIKE; rewrite to LOWER(col) LIKE LOWER(pattern)
    if dialect == "databend":
        def _ilike_replace(match: re.Match) -> str:
            lhs, rhs = match.group(1), match.group(2)
            return f"LOWER({lhs}) LIKE LOWER({rhs})"

        sql = re.sub(r"([A-Za-z0-9_.]+)\s+ILIKE\s+('[^']*')", _ilike_replace, sql, flags=re.IGNORECASE)

    return flatten_sql(sql)


def process_tpch_queries(input_dir: Path, output_dir: Path, database: str) -> list:
    """Process TPC-H queries and return list for input JSON."""
    queries = []

    for i in range(1, 23):  # TPC-H has 22 queries
        query_file = input_dir / f"{i}.sql"
        if not query_file.exists():
            print(f"Warning: {query_file} not found")
            continue

        sql = query_file.read_text()
        converted = convert_tpch_query(sql, str(i))

        queries.append({
            "query": f"{converted}@{database}"
        })

    return queries


def process_tpcds_queries(input_dir: Path, output_dir: Path, database: str) -> list:
    """Process TPC-DS queries and return list for input JSON."""
    queries = []

    for query_file in sorted(input_dir.glob("query*.sql")):
        sql = query_file.read_text()
        converted = convert_tpcds_query(sql)

        queries.append({
            "query": f"{converted}@{database}"
        })

    return queries


def process_job_queries(input_dir: Path, output_dir: Path, database: str) -> list:
    """Process JOB queries and return list for input JSON."""
    queries = []

    for query_file in sorted(input_dir.glob("*.sql")):
        sql = query_file.read_text()
        converted = convert_job_query(sql)

        queries.append({
            "query": f"{converted}@{database}"
        })

    return queries


def process_ceb_queries(input_dir: Path, database: str, dialect: str) -> list:
    """Process CEB queries (across all subfolders) and return list for input JSON."""
    queries = []

    for folder in sorted(p for p in input_dir.iterdir() if p.is_dir()):
        for query_file in sorted(folder.glob("*.sql")):
            sql = query_file.read_text()
            converted = convert_ceb_query(sql, dialect)
            queries.append({
                "query": f"{converted}@{database}"
            })

    return queries


def main():
    base_dir = Path(__file__).parent.parent.parent  # PBench root
    sql_queries_dir = base_dir / "sql_queries"
    output_dir = Path(__file__).parent / "metrics_witho" / "input"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process TPC-H queries
    print("Processing TPC-H queries...")
    tpch_queries = process_tpch_queries(
        sql_queries_dir / "tpch",
        output_dir,
        "tpch1g"  # Default database
    )
    with open(output_dir / "TPCH-tpch1g-sql-input.json", "w") as f:
        json.dump(tpch_queries, f, indent=2)
    print(f"  Created {len(tpch_queries)} TPC-H queries")

    # Process TPC-DS queries
    print("Processing TPC-DS queries...")
    tpcds_queries = process_tpcds_queries(
        sql_queries_dir / "tpcds",
        output_dir,
        "tpcds1g"  # Default database
    )
    with open(output_dir / "tpcds_all-tpcds1g-sql-input.json", "w") as f:
        json.dump(tpcds_queries, f, indent=2)
    print(f"  Created {len(tpcds_queries)} TPC-DS queries")

    # Process JOB queries
    print("Processing JOB queries...")
    job_queries = process_job_queries(
        sql_queries_dir / "job",
        output_dir,
        "imdb"  # Default database
    )
    with open(output_dir / "imdb-imdb-sql-input.json", "w") as f:
        json.dump(job_queries, f, indent=2)
    print(f"  Created {len(job_queries)} JOB queries")

    # Process CEB queries (IMDB schema, no dialect-specific rewrites yet)
    print("Processing CEB queries...")
    ceb_queries_databend = process_ceb_queries(
        sql_queries_dir / "ceb",
        "imdb",
        dialect="databend"
    )
    with open(output_dir / "ceb-imdb-sql-input.json", "w") as f:
        json.dump(ceb_queries_databend, f, indent=2)
    print(f"  Created {len(ceb_queries_databend)} CEB queries (Databend/default)")

    # If a dialect needs special handling later, generate dedicated files here.
    ceb_queries_postgres = process_ceb_queries(
        sql_queries_dir / "ceb",
        "imdb",
        dialect="postgres"
    )
    with open(output_dir / "ceb-imdb-sql-input-postgres.json", "w") as f:
        json.dump(ceb_queries_postgres, f, indent=2)

    ceb_queries_duckdb = process_ceb_queries(
        sql_queries_dir / "ceb",
        "imdb",
        dialect="duckdb"
    )
    with open(output_dir / "ceb-imdb-sql-input-duckdb.json", "w") as f:
        json.dump(ceb_queries_duckdb, f, indent=2)
    print(f"  Created CEB queries for Postgres and DuckDB (currently identical)")

    print("\nDone! Input files created in:", output_dir)


if __name__ == "__main__":
    main()
