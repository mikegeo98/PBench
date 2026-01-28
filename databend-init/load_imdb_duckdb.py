#!/usr/bin/env python3
"""
Load IMDB/JOB data into DuckDB.

This script loads the IMDB data from CSV files into a DuckDB database.
The CSV files should be downloaded first using load_imdb.sh (which also
preprocesses them to remove trailing $ characters).

Usage:
    python load_imdb_duckdb.py [database_path] [data_dir]

Examples:
    python load_imdb_duckdb.py imdb.duckdb
    python load_imdb_duckdb.py imdb.duckdb ./imdb-data
"""

import sys
import time
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


# IMDB schema for DuckDB
SCHEMA = """
CREATE TABLE aka_name (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    name VARCHAR,
    imdb_index VARCHAR(12),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE aka_title (
    id INTEGER PRIMARY KEY,
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
);

CREATE TABLE cast_info (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    person_role_id INTEGER,
    note VARCHAR,
    nr_order INTEGER,
    role_id INTEGER NOT NULL
);

CREATE TABLE char_name (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE comp_cast_type (
    id INTEGER PRIMARY KEY,
    kind VARCHAR(32) NOT NULL
);

CREATE TABLE company_name (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    country_code VARCHAR(255),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    name_pcode_sf VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE company_type (
    id INTEGER PRIMARY KEY,
    kind VARCHAR(32)
);

CREATE TABLE complete_cast (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER,
    subject_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL
);

CREATE TABLE info_type (
    id INTEGER PRIMARY KEY,
    info VARCHAR(32) NOT NULL
);

CREATE TABLE keyword (
    id INTEGER PRIMARY KEY,
    keyword VARCHAR NOT NULL,
    phonetic_code VARCHAR(5)
);

CREATE TABLE kind_type (
    id INTEGER PRIMARY KEY,
    kind VARCHAR(15)
);

CREATE TABLE link_type (
    id INTEGER PRIMARY KEY,
    link VARCHAR(32) NOT NULL
);

CREATE TABLE movie_companies (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    company_type_id INTEGER NOT NULL,
    note VARCHAR
);

CREATE TABLE movie_info (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR
);

CREATE TABLE movie_info_idx (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR(1)
);

CREATE TABLE movie_keyword (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL
);

CREATE TABLE movie_link (
    id INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    linked_movie_id INTEGER NOT NULL,
    link_type_id INTEGER NOT NULL
);

CREATE TABLE name (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    imdb_id INTEGER,
    gender VARCHAR(1),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE person_info (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR
);

CREATE TABLE role_type (
    id INTEGER PRIMARY KEY,
    role VARCHAR(32) NOT NULL
);

CREATE TABLE title (
    id INTEGER PRIMARY KEY,
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
);
"""

# Tables in load order (smaller tables first for faster feedback)
TABLES = [
    "comp_cast_type", "company_type", "info_type", "kind_type", "link_type", "role_type",
    "aka_name", "aka_title", "char_name", "company_name", "keyword", "name", "title",
    "cast_info", "complete_cast", "movie_companies", "movie_info", "movie_info_idx",
    "movie_keyword", "movie_link", "person_info"
]

# Expected row counts
EXPECTED_COUNTS = {
    "aka_name": 901343,
    "aka_title": 361472,
    "cast_info": 36244344,
    "char_name": 3140339,
    "company_name": 234997,
    "company_type": 4,
    "comp_cast_type": 4,
    "complete_cast": 135086,
    "info_type": 113,
    "keyword": 134170,
    "kind_type": 7,
    "link_type": 18,
    "movie_companies": 2609129,
    "movie_info": 14835720,
    "movie_info_idx": 1380035,
    "movie_keyword": 4523930,
    "movie_link": 29997,
    "name": 4167491,
    "person_info": 2963664,
    "role_type": 12,
    "title": 2528312
}


def main():
    script_dir = Path(__file__).parent

    # Parse arguments
    db_path = sys.argv[1] if len(sys.argv) > 1 else "imdb.duckdb"
    data_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else script_dir / "imdb-data"

    print("IMDB/JOB DuckDB Data Loader")
    print("=" * 50)
    print(f"Database: {db_path}")
    print(f"Data dir: {data_dir}")
    print("=" * 50)

    # Check if data directory exists
    if not data_dir.exists():
        print(f"\nERROR: Data directory not found: {data_dir}")
        print("Please run load_imdb.sh first to download the IMDB data:")
        print("  ./load_imdb.sh")
        sys.exit(1)

    # Check for CSV files
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"\nERROR: No CSV files found in {data_dir}")
        print("Please run load_imdb.sh first to download the IMDB data")
        sys.exit(1)

    # Remove existing database if it exists
    db_file = Path(db_path)
    if db_file.exists():
        print(f"\nRemoving existing database: {db_path}")
        db_file.unlink()

    # Connect to DuckDB
    print("\nConnecting to DuckDB...")
    conn = duckdb.connect(db_path)

    # Create schema
    print("Creating tables...")
    for stmt in SCHEMA.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    # Load data
    print("\nLoading data from CSV files...")
    total_rows = 0
    total_time = 0

    for table in TABLES:
        csv_path = data_dir / f"{table}.csv"
        if not csv_path.exists():
            print(f"  {table:18} SKIPPED (file not found)")
            continue

        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"  {table:18} ({size_mb:>7.1f} MB)... ", end="", flush=True)

        start = time.time()
        try:
            # DuckDB can read CSV directly with auto-detection
            conn.execute(f"""
                INSERT INTO {table}
                SELECT * FROM read_csv('{csv_path}',
                    header=false,
                    quote='"',
                    escape='"',
                    null_padding=true,
                    ignore_errors=true
                )
            """)
            elapsed = time.time() - start
            total_time += elapsed

            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            total_rows += count

            expected = EXPECTED_COUNTS.get(table, 0)
            status = "OK" if count == expected else f"(expected {expected:,})"
            print(f"{count:>12,} rows in {elapsed:>5.1f}s {status}")

        except Exception as e:
            print(f"FAILED: {e}")

    # Summary
    print("\n" + "-" * 50)
    print(f"  {'TOTAL':18} {total_rows:>12,} rows in {total_time:.1f}s")

    # Show database size
    conn.close()
    db_size = db_file.stat().st_size / (1024 * 1024)
    print(f"\nDatabase size: {db_size:.1f} MB")

    print("\n" + "=" * 50)
    print("Done!")
    print(f"\nTo use with collect.py:")
    print(f"  python collect.py imdb --no-databend --duckdb --duckdb-path {db_path}")


if __name__ == "__main__":
    main()
