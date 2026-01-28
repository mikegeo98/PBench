#!/bin/bash
# Load IMDB/JOB data into PostgreSQL
#
# This script loads IMDB data from CSV files into PostgreSQL.
# The CSV files should be downloaded first using load_imdb.sh.
#
# Usage:
#   ./load_imdb_postgres.sh [database] [host] [port]
#
# Examples:
#   ./load_imdb_postgres.sh imdb
#   ./load_imdb_postgres.sh imdb localhost 5432

set -e

DATABASE=${1:-imdb}
HOST=${2:-localhost}
PORT=${3:-5432}
USER=${PGUSER:-postgres}
PASSWORD=${PGPASSWORD:-postgres}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/imdb-data"

export PGPASSWORD="${PASSWORD}"

echo "IMDB/JOB PostgreSQL Data Loader"
echo "========================================"
echo "Database: ${DATABASE}"
echo "Host: ${HOST}:${PORT}"
echo "User: ${USER}"
echo "Data dir: ${DATA_DIR}"
echo "========================================"

# Check if data exists
if [ ! -f "${DATA_DIR}/title.csv" ]; then
    echo ""
    echo "ERROR: IMDB data files not found in ${DATA_DIR}"
    echo "Please download data first using:"
    echo "  ./load_imdb.sh"
    exit 1
fi

# Create database if it doesn't exist
echo ""
echo "Step 1: Creating database..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d postgres -c "DROP DATABASE IF EXISTS ${DATABASE};" 2>/dev/null || true
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d postgres -c "CREATE DATABASE ${DATABASE};"
echo "  Created database: ${DATABASE}"

# Create schema
echo ""
echo "Step 2: Creating IMDB schema..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" << 'EOSQL'
-- IMDB/JOB Schema for PostgreSQL

CREATE TABLE aka_name (
    id INTEGER NOT NULL PRIMARY KEY,
    person_id INTEGER NOT NULL,
    name VARCHAR,
    imdb_index VARCHAR(12),
    name_pcode_cf VARCHAR(5),
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE aka_title (
    id INTEGER NOT NULL PRIMARY KEY,
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
    id INTEGER NOT NULL PRIMARY KEY,
    person_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    person_role_id INTEGER,
    note VARCHAR,
    nr_order INTEGER,
    role_id INTEGER NOT NULL
);

CREATE TABLE char_name (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    imdb_index VARCHAR(12),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    surname_pcode VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE comp_cast_type (
    id INTEGER NOT NULL PRIMARY KEY,
    kind VARCHAR(32) NOT NULL
);

CREATE TABLE company_name (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    country_code VARCHAR(255),
    imdb_id INTEGER,
    name_pcode_nf VARCHAR(5),
    name_pcode_sf VARCHAR(5),
    md5sum VARCHAR(32)
);

CREATE TABLE company_type (
    id INTEGER NOT NULL PRIMARY KEY,
    kind VARCHAR(32)
);

CREATE TABLE complete_cast (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER,
    subject_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL
);

CREATE TABLE info_type (
    id INTEGER NOT NULL PRIMARY KEY,
    info VARCHAR(32) NOT NULL
);

CREATE TABLE keyword (
    id INTEGER NOT NULL PRIMARY KEY,
    keyword VARCHAR NOT NULL,
    phonetic_code VARCHAR(5)
);

CREATE TABLE kind_type (
    id INTEGER NOT NULL PRIMARY KEY,
    kind VARCHAR(15)
);

CREATE TABLE link_type (
    id INTEGER NOT NULL PRIMARY KEY,
    link VARCHAR(32) NOT NULL
);

CREATE TABLE movie_companies (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    company_type_id INTEGER NOT NULL,
    note VARCHAR
);

CREATE TABLE movie_info (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR
);

CREATE TABLE movie_info_idx (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR(1)
);

CREATE TABLE movie_keyword (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL
);

CREATE TABLE movie_link (
    id INTEGER NOT NULL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    linked_movie_id INTEGER NOT NULL,
    link_type_id INTEGER NOT NULL
);

CREATE TABLE name (
    id INTEGER NOT NULL PRIMARY KEY,
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
    id INTEGER NOT NULL PRIMARY KEY,
    person_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    info VARCHAR NOT NULL,
    note VARCHAR
);

CREATE TABLE role_type (
    id INTEGER NOT NULL PRIMARY KEY,
    role VARCHAR(32) NOT NULL
);

CREATE TABLE title (
    id INTEGER NOT NULL PRIMARY KEY,
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
EOSQL
echo "  Schema created"

# Load data
echo ""
echo "Step 3: Loading data..."

# Tables in load order (smaller tables first)
TABLES="comp_cast_type company_type info_type kind_type link_type role_type aka_name aka_title char_name company_name keyword name title cast_info complete_cast movie_companies movie_info movie_info_idx movie_keyword movie_link person_info"

for t in ${TABLES}; do
    CSV_FILE="${DATA_DIR}/${t}.csv"
    if [ ! -f "${CSV_FILE}" ]; then
        echo "  ${t}: SKIPPED (file not found)"
        continue
    fi

    SIZE=$(ls -lh "${CSV_FILE}" | awk '{print $5}')
    echo -n "  ${t} (${SIZE})... "

    START=$(date +%s.%N)

    # PostgreSQL COPY command - IMDB CSV files use comma delimiter with quotes
    psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" -c "\COPY ${t} FROM '${CSV_FILE}' WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', NULL '')" 2>/dev/null

    END=$(date +%s.%N)
    ELAPSED=$(echo "$END - $START" | bc)

    COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" -t -c "SELECT COUNT(*) FROM ${t};" | tr -d ' ')
    echo "${COUNT} rows in ${ELAPSED}s"
done

# Verify
echo ""
echo "Step 4: Verifying row counts..."

declare -A EXPECTED
EXPECTED[aka_name]=901343
EXPECTED[aka_title]=361472
EXPECTED[cast_info]=36244344
EXPECTED[char_name]=3140339
EXPECTED[company_name]=234997
EXPECTED[company_type]=4
EXPECTED[comp_cast_type]=4
EXPECTED[complete_cast]=135086
EXPECTED[info_type]=113
EXPECTED[keyword]=134170
EXPECTED[kind_type]=7
EXPECTED[link_type]=18
EXPECTED[movie_companies]=2609129
EXPECTED[movie_info]=14835720
EXPECTED[movie_info_idx]=1380035
EXPECTED[movie_keyword]=4523930
EXPECTED[movie_link]=29997
EXPECTED[name]=4167491
EXPECTED[person_info]=2963664
EXPECTED[role_type]=12
EXPECTED[title]=2528312

TOTAL=0
for t in ${TABLES}; do
    COUNT=$(psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" -t -c "SELECT COUNT(*) FROM ${t};" 2>/dev/null | tr -d ' ')
    COUNT=${COUNT:-0}
    EXP=${EXPECTED[$t]:-?}
    if [ "${COUNT}" = "${EXP}" ]; then
        STATUS="OK"
    elif [ "${COUNT}" != "0" ]; then
        STATUS="(expected ${EXP})"
    else
        STATUS=""
    fi
    printf "  %-18s %'12d %s\n" "${t}:" "${COUNT}" "${STATUS}"
    TOTAL=$((TOTAL + COUNT))
done

echo ""
printf "  %-18s %'12d\n" "TOTAL:" "${TOTAL}"

# Create indexes for better query performance
echo ""
echo "Step 5: Creating indexes..."
psql -h "${HOST}" -p "${PORT}" -U "${USER}" -d "${DATABASE}" << 'EOSQL'
-- Foreign key indexes for JOB queries
CREATE INDEX idx_aka_name_person_id ON aka_name(person_id);
CREATE INDEX idx_aka_title_movie_id ON aka_title(movie_id);
CREATE INDEX idx_aka_title_kind_id ON aka_title(kind_id);
CREATE INDEX idx_cast_info_person_id ON cast_info(person_id);
CREATE INDEX idx_cast_info_movie_id ON cast_info(movie_id);
CREATE INDEX idx_cast_info_role_id ON cast_info(role_id);
CREATE INDEX idx_cast_info_person_role_id ON cast_info(person_role_id);
CREATE INDEX idx_complete_cast_movie_id ON complete_cast(movie_id);
CREATE INDEX idx_complete_cast_subject_id ON complete_cast(subject_id);
CREATE INDEX idx_complete_cast_status_id ON complete_cast(status_id);
CREATE INDEX idx_movie_companies_movie_id ON movie_companies(movie_id);
CREATE INDEX idx_movie_companies_company_id ON movie_companies(company_id);
CREATE INDEX idx_movie_companies_company_type_id ON movie_companies(company_type_id);
CREATE INDEX idx_movie_info_movie_id ON movie_info(movie_id);
CREATE INDEX idx_movie_info_info_type_id ON movie_info(info_type_id);
CREATE INDEX idx_movie_info_idx_movie_id ON movie_info_idx(movie_id);
CREATE INDEX idx_movie_info_idx_info_type_id ON movie_info_idx(info_type_id);
CREATE INDEX idx_movie_keyword_movie_id ON movie_keyword(movie_id);
CREATE INDEX idx_movie_keyword_keyword_id ON movie_keyword(keyword_id);
CREATE INDEX idx_movie_link_movie_id ON movie_link(movie_id);
CREATE INDEX idx_movie_link_linked_movie_id ON movie_link(linked_movie_id);
CREATE INDEX idx_movie_link_link_type_id ON movie_link(link_type_id);
CREATE INDEX idx_person_info_person_id ON person_info(person_id);
CREATE INDEX idx_person_info_info_type_id ON person_info(info_type_id);
CREATE INDEX idx_title_kind_id ON title(kind_id);
CREATE INDEX idx_title_production_year ON title(production_year);
ANALYZE;
EOSQL
echo "  Indexes created and statistics updated"

echo ""
echo "========================================"
echo "Done!"
echo ""
echo "To use with collect.py:"
echo "  export PGPASSWORD=postgres"
echo "  python collect.py imdb --postgres --pg-database ${DATABASE}"
