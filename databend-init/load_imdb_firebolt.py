#!/usr/bin/env python3
"""
Load TPC-H data into Firebolt-Core.

This script expects pre-generated TPC-H .tbl files in:
  databend-init/tpch-data/sf<SCALE>/

Usage:
    python load_tpch_firebolt.py [scale_factor] [database] [host] [port]

Examples:
    python load_tpch_firebolt.py 1 tpch1g
    python load_tpch_firebolt.py 1 tpch1g localhost 3473
"""

from __future__ import annotations

import sys
import time
import re
from pathlib import Path

import requests


TABLES = {
    "aka_name": "CREATE TABLE aka_name (id INTEGER NOT NULL,person_id INTEGER NOT NULL,name VARCHAR,imdb_index VARCHAR(12),name_pcode_cf VARCHAR(5),name_pcode_nf VARCHAR(5),surname_pcode VARCHAR(5),md5sum VARCHAR(32)) PRIMARY INDEX id;", 
    "aka_title": """CREATE TABLE aka_title (
    id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    title VARCHAR,
    imdb_index VARCHAR(12),
    kind_id INTEGER NOT NULL,
    production_year INTEGER,
    phonetic_code VARCHAR(5),
    episode_of_id INTEGER,
    season_nr INTEGER,
    episode_nr INTEGER,
    note VARCHAR,
    md5sum VARCHAR(32)
) PRIMARY INDEX id;""", 
    "cast_info": """CREATE TABLE cast_info (
    id INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    person_role_id INTEGER,
    note VARCHAR,
    nr_order INTEGER,
    role_id INTEGER NOT NULL
) PRIMARY INDEX id;""", 
    "char_name": """CREATE TABLE char_name (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
) PRIMARY INDEX id;""", 
    "comp_cast_type": "CREATE TABLE comp_cast_type (id INTEGER NOT NULL,kind VARCHAR(32) NOT NULL) PRIMARY INDEX id;", 
    "company_name": """CREATE TABLE company_name (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    country_code VARCHAR(255),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    name_pcode_sf VARCHAR(5),
    md5sum VARCHAR(32)
) PRIMARY INDEX id;""", 
    "company_type": "CREATE TABLE company_type (id INTEGER NOT NULL,kind VARCHAR(32)) PRIMARY INDEX id;", 
    "complete_cast": """CREATE TABLE complete_cast (
    id INTEGER NOT NULL,
    movie_id INTEGER,
    subject_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL
) PRIMARY INDEX id;""", 
    "info_type": "CREATE TABLE info_type (id INTEGER NOT NULL,info VARCHAR(32) NOT NULL) PRIMARY INDEX id;", 
    "keyword": "CREATE TABLE keyword (id INTEGER NOT NULL,keyword VARCHAR NOT NULL,phonetic_code VARCHAR(5)) PRIMARY INDEX id;",
    "kind_type": "CREATE TABLE kind_type (id INTEGER NOT NULL,kind VARCHAR(15)) PRIMARY INDEX id;", 
    "link_type": "CREATE TABLE link_type (id INTEGER NOT NULL,link VARCHAR(32) NOT NULL) PRIMARY INDEX id;", 
    "movie_companies": "CREATE TABLE movie_companies (id INTEGER NOT NULL,movie_id INTEGER NOT NULL,company_id INTEGER NOT NULL,company_type_id INTEGER NOT NULL,note VARCHAR) PRIMARY INDEX id;", 
    "movie_info": "CREATE TABLE movie_info (id INTEGER NOT NULL,movie_id INTEGER NOT NULL,info_type_id INTEGER NOT NULL,info VARCHAR NOT NULL,note VARCHAR) PRIMARY INDEX id;", 
    "movie_info_idx": "CREATE TABLE movie_info_idx (id INTEGER NOT NULL,movie_id INTEGER NOT NULL,info_type_id INTEGER NOT NULL,info VARCHAR NOT NULL,note VARCHAR(1)) PRIMARY INDEX id;", 
    "movie_keyword": "CREATE TABLE movie_keyword (id INTEGER NOT NULL,movie_id INTEGER NOT NULL,keyword_id INTEGER NOT NULL) PRIMARY INDEX id;", 
    "movie_link": """CREATE TABLE movie_link (
    id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    linked_movie_id INTEGER NOT NULL,
    link_type_id INTEGER NOT NULL
) PRIMARY INDEX id;""", 
    "name": """CREATE TABLE name (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    imdb_id INTEGER,
    gender VARCHAR(1),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
) PRIMARY INDEX id;""", 
    "person_info": "CREATE TABLE person_info (id INTEGER NOT NULL,person_id INTEGER NOT NULL,info_type_id INTEGER NOT NULL,info VARCHAR NOT NULL,note VARCHAR) PRIMARY INDEX id;",
    "role_type": "CREATE TABLE role_type (id INTEGER NOT NULL,role VARCHAR(32) NOT NULL) PRIMARY INDEX id;", 
    "title": """CREATE TABLE title (
    id INTEGER NOT NULL,
    title VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    kind_id INTEGER NOT NULL,
    production_year INTEGER,
    imdb_id INTEGER,
    phonetic_code VARCHAR(5),
    episode_of_id INTEGER,
    season_nr INTEGER,
    episode_nr INTEGER,
    series_years VARCHAR(49),
    md5sum VARCHAR(32)
) PRIMARY INDEX id;"""
}

def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def run_sql(api_url: str, sql: str) -> str:
    response = requests.post(api_url, data=sql.encode("utf-8"), timeout=300)
    if response.status_code >= 400:
        die(f"ERROR running SQL: {response.status_code}\n{response.text.strip()}\n{sql}")
    return response.text.strip()


def count_rows(api_url: str, table: str) -> int:
    raw = run_sql(api_url, f"SELECT COUNT(*) FROM {table};")
    for line in raw.splitlines():
        cleaned = line.strip().replace(",", "")
        if re.fullmatch(r"\d+", cleaned):
            return int(cleaned)
    die(f"ERROR: unexpected count result for table {table}: {raw}")

def human_size(path: Path) -> str:
    size = path.stat().st_size
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}{unit}"
        value /= 1024.0
    return f"{size}B"


def main() -> None:
    database = sys.argv[2] if len(sys.argv) > 2 else "tpch1g"
    host = sys.argv[3] if len(sys.argv) > 3 else "localhost"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 3473

    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "job"
    api_url = f"http://{host}:{port}/?output_format=psql&database={database}"

    print("JOB Firebolt-Core Data Loader")
    print("========================================")
    print(f"Database: {database}")
    print(f"SQL API: {host}:{port}")
    print(f"Data dir: {data_dir}")
    print("========================================")

    print("\nStep 1: Creating database...")
    run_sql(api_url, f"CREATE DATABASE IF NOT EXISTS {database};")

    print("\nStep 2: Creating JOB schema...")
    for statement in TABLES.values():
        run_sql(api_url, statement)
    print("  Schema created")

    print("\nStep 3: Loading data...")
    for table in TABLES.keys():
        start = time.time()
        run_sql(api_url, f"COPY {table} FROM 's3://imdb/{table}.csv' WITH(CREDENTIALS=(AWS_ACCESS_KEY_ID ='minioadmin',AWS_SECRET_ACCESS_KEY = 'minioadmin'),HEADER = FALSE, TYPE = csv, DELIMITER=',');")
        elapsed = time.time() - start
        count = count_rows(api_url, table)
        print(f"{count} rows in {elapsed:.3f}s")

    EXPECTED = {
    'aka_name': 901343,
    'aka_title': 361472,
    'cast_info': 36244344,
    'char_name': 3140339,
    'company_name': 234997,
    'company_type': 4,
    'comp_cast_type': 4,
    'complete_cast': 135086,
    'info_type': 113,
    'keyword': 134170,
    'kind_type': 7,
    'link_type': 18,
    'movie_companies': 2609129,
    'movie_info': 14835720,
    'movie_info_idx': 1380035,
    'movie_keyword': 4523930,
    'movie_link': 29997,
    'name': 4167491,
    'person_info': 2963664,
    'role_type': 12,
    'title': 2528312
    }

    print("\nStep 4: Verifying row counts...")
    total = 0
    for table in TABLES.keys():
        count = count_rows(api_url, table)
        expected = EXPECTED[table]
        status = "OK" if count == expected else f"(expected {expected})"
        print(f"  {table + ':':12} {count:>12,} {status}")
        total += count

    print(f"\n  {'TOTAL:':12} {total:>12,}")

    print("\nStep 5: Creating indexes...")
    print("  Index creation skipped (unsupported)")

    print("\n========================================")
    print("Done!")


if __name__ == "__main__":
    main()
