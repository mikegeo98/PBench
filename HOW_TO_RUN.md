# How to Run PBench

This guide explains how to set up and run PBench for database workload synthesis and benchmarking.

**Note**: This repository includes sample Snowset workload data and query metrics in `src/Workloads/Snowset/` and `src/Collect_metrics/metrics_witho/output/` respectively, so you can run PBench immediately for testing.

## Prerequisites

- Python 3.10 (required)
- Docker and Docker Compose
- Git
- PostgreSQL 12+ (optional, for multi-database metrics collection)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd PBench
```

### 2. Set Up Python Virtual Environment

```bash
# Use uv
uv sync
# or standard venv
python3.10 -m venv .venv
source .venv/bin/activate  # On Linux/Mac, or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 3. Start Databend Database and Initialize Databases

```bash
docker compose up -d
```

The `databend-init` container waits for Databend to be healthy and then runs `databend-init/run_ddl.py` to apply the benchmark DDLs (TPCH, TPCDS, IMDB/JOB) once.

Wait for containers to be healthy (~15-30 seconds):
```bash
docker compose ps
```
If you need to rerun the DDLs manually (e.g., after resetting Databend data):
```bash
cd databend-init
python run_ddl.py --host localhost --port 8000
```

### 4. Set Up PostgreSQL (Optional)

For multi-database metrics collection, set up a local PostgreSQL instance:

```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt install postgresql postgresql-contrib

# Verify it's running
sudo systemctl status postgresql

# The loaders use peer authentication by default (your Unix username)
# Create databases for benchmarks:
createdb tpch1g
createdb tpcds1g
createdb imdb
```

**Load benchmark data into PostgreSQL:**
```bash
cd databend-init

# TPC-H
./load_tpch_postgres.sh 1 tpch1g

# TPC-DS (requires dsdgen - run setup_tpcds_dsdgen.sh first)
./load_tpcds_postgres.sh 1 tpcds1g

# IMDB/JOB
./load_imdb_postgres.sh
```

**Environment variables** (if not using peer auth):
```bash
export PGUSER=myuser
export PGPASSWORD=mypassword
```

## Running PBench

### Complete Workflow

```bash
source .venv/bin/activate
cd src/PBench-tool
python run_pbench.py
```

This will:
1. Process all configuration files in `configs/`
2. Run ILP (Integer Linear Programming) workload generation
3. Generate simulated annealing optimizations
4. Replay and test the generated workloads

### Run Baseline Experiments

The baseline module supports CAB and Stitcher.

#### 1. Initialize Benchmark Tables (Optional, if not done during container initialization)

First, create the benchmark tables in Databend:

```bash
cd databend-init
python run_ddl.py
```

This executes the DDL scripts to create tables for TPC-H, TPC-DS, and IMDB/JOB benchmarks.

#### 2. Generate and Load TPC-H Data

Load TPC-H Scale Factor 1 data (~1GB, 8.6M rows) using the official dbgen tool via Docker:

```bash
cd databend-init
./load_tpch_dbgen.sh 1 tpch1g
cd ..
```

This will:
1. Pull the TPC-H dbgen Docker image
2. Generate official TPC-H SF1 data files
3. Upload and load data into Databend using COPY INTO

#### 2b. Generate and Load TPC-DS Data

Load TPC-DS Scale Factor 1 data (~1GB, 19.5M rows) using the official dsdgen tool:

```bash
cd databend-init

# First time only: build the dsdgen tool
./setup_tpcds_dsdgen.sh

# Generate and load data into Databend
./load_tpcds_dbgen.sh 1 tpcds1g

cd ..
```

This will:
1. Clone and build the TPC-DS dsdgen tool (first time only)
2. Generate official TPC-DS SF1 data files
3. Upload and load data into Databend using COPY INTO

**For PostgreSQL:**
```bash
./load_tpcds_postgres.sh 1 tpcds1g
```

**For DuckDB:**
```bash
python load_tpcds_duckdb.py tpcds1g.duckdb 1
```

#### 2c. Generate and Load IMDB/JOB Data

Load the IMDB dataset for the Join Order Benchmark (~3.7GB, 36M+ rows):

```bash
cd databend-init
./load_imdb.sh
cd ..
```

This will:
1. Download IMDB data from CWI (~1.2GB compressed)
2. Preprocess CSV files (remove trailing $ characters)
3. Create the `imdb` database with 21 tables
4. Load all data using COPY INTO

#### 3. Configure Your Experiment

Create a YAML config file in `src/Baseline/configs/`. Example:

```yaml
workload_path: ../../src/Workloads/Snowset/workload1h-5m-30s_1.csv
workload_name: my-experiment
host: localhost
databend_port: 8000
prometheus_port: 9091
count_limit: 100
time_limit: 60
use_operator: 1
wait: 2
interval: 10
query:
  - TPCH
  - imdb
db:
  - tpch1g
  - imdb
op_scale: 100
initial_count: 5
use_duration: 0
seed: 42
iter: 5                    # Bayesian optimization iterations
seconds_in_time_slot: 60
plan: stitcher             # Options: stitcher, cab
```

**Available query pools:**
| Query Pool | Database | Description |
|------------|----------|-------------|
| `TPCH` | `tpch1g`, `tpch5g`, etc. | TPC-H benchmark queries |
| `tpcds_all` | `tpcds1g`, `tpcds2g` | TPC-DS benchmark queries |
| `imdb` | `imdb` | Join Order Benchmark (JOB) queries |

#### 4. Run the Baseline

```bash
source .venv/bin/activate
cd src/Baseline
python do_baseline.py
```

The script processes all configs in `src/Baseline/configs/` and runs:
- **CAB**: Generates and replays workload plans based on configuration
- **Stitcher**: Uses Bayesian optimization to find optimal workload configurations

Output plans are saved to `src/Baseline/output/`.

### Collect Metrics

Collect query execution metrics (CPU time, scan bytes, duration, operators) from multiple database backends.

```bash
source .venv/bin/activate
cd src/Collect_metrics

# Collect TPC-H metrics (22 queries)
python collect.py tpch

# Collect IMDB/JOB metrics (113 queries)
python collect.py imdb

# Collect TPC-DS metrics
python collect.py tpcds
```

**Options:**
```bash
python collect.py imdb --repeat 1     # Run each query once (faster, less accurate)
python collect.py imdb --repeat 5     # Run each query 5 times (more accurate)
python collect.py imdb --start 10     # Resume/start from query index 10

# Single-database collection (--postgres/--duckdb alone disables Databend)
python collect.py tpch --databend     # Databend only (default)
python collect.py tpch --no-databend --postgres     # PostgreSQL only
python collect.py tpch --no-databend --duckdb       # DuckDB only

# Multi-database collection
python collect.py tpch --databend --postgres  # Databend + PostgreSQL
python collect.py tpch --databend --duckdb    # Databend + DuckDB
python collect.py tpch --all                  # All three databases

# DuckDB with custom path
python collect.py tpcds --duckdb --duckdb-path ./tpcds1g.duckdb

# PostgreSQL + DuckDB without Databend
python collect.py tpcds --no-databend --postgres --duckdb
```

**Prerequisites:**
- Databend running with data loaded (TPC-H, IMDB, or TPC-DS)
- Prometheus scraping Databend metrics (port 9091)
- `.env` file configured in `src/Collect_metrics/`:
  ```
  HOST=localhost
  DATABEND_PORT=8000
  PROMETHEUS_PORT=9091
  DUCKDB_PATH=./tpcds1g.duckdb    # Path to DuckDB database file
  PG_DATABASE=tpcds1g             # PostgreSQL database name
  ```

**Output:**
- `metrics_witho/output/TPCH-tpch1g-sql-metrics.json` - TPC-H metrics (Databend)
- `metrics_witho/output/TPCH-tpch1g-sql-metrics-postgres.json` - TPC-H metrics (PostgreSQL)
- `metrics_witho/output/TPCH-tpch1g-sql-metrics-duckdb.json` - TPC-H metrics (DuckDB)
- `metrics_witho/output/imdb-imdb-sql-metrics.json` - IMDB metrics
- `metrics_witho/output/tpcds_all-tpcds1g-sql-metrics.json` - TPC-DS metrics

The script automatically resumes from where it left off if interrupted.

## Configuration

Configuration files are located in `src/PBench-tool/configs/`. Each workload has its own subdirectory.

**All configuration files are pre-configured for localhost.** To connect to a remote Databend instance, update the `host`, `databend_port`, and `prometheus_port` values in the YAML files.

### Example Configuration

```yaml
workload_path: "../../src/Workloads/Snowset/workload1h-5m-30s_1.csv"
workload_name: "workload1h-5m-30s_1"
host: "localhost"              # Change if using remote Databend
databend_port: 8000
prometheus_port: 9090
count_limit: 1000
time_limit: 270
use_operator: 1
interval: 30
query: [TPCH, TPCH, tpcds_all, imdb, llm]
db: [tpch1g, tpch5g, tpcds1g, imdb, llm]
```


### Custom Query Metrics

To collect metrics from your own benchmark databases:

1. **Load your benchmark data into Databend**
   ```bash
   # TPC-H
   cd databend-init && ./load_tpch_dbgen.sh 1 tpch1g

   # TPC-DS (first time: run ./setup_tpcds_dsdgen.sh)
   cd databend-init && ./load_tpcds_dbgen.sh 1 tpcds1g

   # IMDB/JOB
   cd databend-init && ./load_imdb.sh
   ```

2. **Collect query statistics**
   ```bash
   cd src/Collect_metrics
   python collect.py tpch    # For TPC-H
   python collect.py imdb    # For IMDB/JOB
   python collect.py tpcds   # For TPC-DS
   ```

3. **Metrics are saved to** `src/Collect_metrics/metrics_witho/output/`

**File format:** `{query_type}-{database}-sql-metrics.json`

Each JSON file contains an array of query metrics:
```json
[
  {
    "query": "SELECT ... @database_name",
    "avg_cpu_time": 0.45,
    "avg_scan_bytes": 1234567,
    "avg_duration": 0.123,
    "filter": 1,
    "join": 1,
    "agg": 1,
    "sort": 0
  }
]
```

## Stopping Services

```bash
docker compose down       # Stop containers
docker compose down -v    # Stop and remove all data
```

## References

- [PBench Paper](https://arxiv.org/abs/2506.16379) - VLDB'25 paper
- [Snowset Dataset](https://github.com/resource-disaggregation/snowset) - Real Snowflake query statistics
- [Databend Documentation](https://databend.rs/) - Database documentation
