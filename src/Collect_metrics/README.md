# Collect_metrics

This folder contains scripts to:

1. generate normalized SQL input JSON files (`convert_queries.py`)
2. execute those queries and collect per-query metrics (`collect.py`)

The produced metrics JSON files are consumed by `src/PBench-tool` during ILP/SA synthesis.

## Scripts

## `convert_queries.py`

Purpose:
- Reads benchmark SQL files from `sql_queries/`
- Applies dialect-specific conversions (especially for Databend)
- Writes query input JSON files to `metrics_witho/input/`

Main assumptions:
- Repository layout is unchanged (script expects `sql_queries/` under repo root).
- Redbench generation requires:
  - `sql_queries/redbench/job`
  - `sql_queries/redbench/ceb`
- For Databend CEB/Redbench conversion:
  - regex operator `~` is rewritten to `REGEXP`
  - `ILIKE` is rewritten to `LOWER(...) LIKE LOWER(...)`
  - PostgreSQL-style `::float` is rewritten to `TRY_CAST(... AS DOUBLE)`

Run:

```bash
cd src/Collect_metrics
python convert_queries.py
```

Main outputs (examples):
- `metrics_witho/input/imdb-imdb-sql-input.json`
- `metrics_witho/input/ceb-imdb-sql-input.json`
- `metrics_witho/input/redbench-imdb-sql-input.json`
- plus postgres/duckdb variants where applicable

## `collect.py`

Purpose:
- Executes queries from `metrics_witho/input/*.json`
- Collects:
  - `avg_cpu_time`
  - `avg_scan_bytes`
  - `avg_duration`
  - operator flags (`filter`, `join`, `agg`, `sort`)
- Writes metrics JSON to `metrics_witho/output/`

Databases supported:
- Databend (default)
- PostgreSQL (optional)
- DuckDB (optional)

Important assumptions:
- Run from `src/Collect_metrics` (paths are relative to this folder).
- Databend + Prometheus are running and reachable.
- Prometheus query definitions come from `src/utils/prometheus.py`.
- For short queries, use a suitable Prometheus scrape interval and collection wait.

Dependencies:
- required: `databend_driver`, `requests`, `python-dotenv`
- optional:
  - `psycopg2-binary` for PostgreSQL
  - `duckdb` for DuckDB

## Environment

Configuration is read from `.env`/environment variables:
- `HOST` (default `localhost`)
- `DATABEND_PORT` (default `8000`)
- `PROMETHEUS_PORT` (default `9091`)
- `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`
- `DUCKDB_PATH`

## Common command examples

Databend only (default):

```bash
cd src/Collect_metrics
python collect.py redbench --databend --repeat 3 --prometheus-wait-seconds 2.0
```

PostgreSQL only:

```bash
cd src/Collect_metrics
python collect.py tpcds --no-databend --postgres --pg-database tpcds1g
```

DuckDB only:

```bash
cd src/Collect_metrics
python collect.py tpcds --no-databend --duckdb --duckdb-path /path/to/tpcds1g.duckdb
```

All three backends:

```bash
cd src/Collect_metrics
python collect.py tpch --all --pg-database tpch1g --duckdb-path /path/to/tpch1g.duckdb
```

## Output files

Databend output (default name from benchmark config):
- `metrics_witho/output/<benchmark>-<database>-sql-metrics.json`

If PostgreSQL/DuckDB are enabled simultaneously:
- PostgreSQL: `...-sql-metrics-postgres.json`
- DuckDB: `...-sql-metrics-duckdb.json`

Example for redbench:
- Databend: `metrics_witho/output/redbench-imdb-sql-metrics.json`

## Resume behavior

`collect.py` resumes from existing output by default:
- If output file already exists, collection continues from the next query index.

To force restart from beginning:
1. delete the old output file, or
2. run with `--start 0`

Example:

```bash
cd src/Collect_metrics
rm -f metrics_witho/output/redbench-imdb-sql-metrics.json
python collect.py redbench --databend --repeat 3 --prometheus-wait-seconds 2.0
```
