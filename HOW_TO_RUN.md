# How to Run PBench

This guide explains how to set up and run PBench for database workload synthesis and benchmarking.

**Note**: This repository includes sample Snowset workload data and query metrics in `src/Workloads/Snowset/` and `src/Collect_metrics/metrics_witho/output/` respectively, so you can run PBench immediately for testing.

## Prerequisites

- Python 3.10 (required)
- Docker and Docker Compose
- Git

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

**Alternative: Quick synthetic data** (for development/testing):
```bash
cd databend-init
python load_tpch_sf1.py
cd ..
```

This uses pure SQL to generate synthetic data with correct row counts but approximate distributions.

**Expected row counts (SF1):**
| Table | Rows |
|-------|------|
| region | 5 |
| nation | 25 |
| supplier | 10,000 |
| part | 200,000 |
| partsupp | 800,000 |
| customer | 150,000 |
| orders | 1,500,000 |
| lineitem | 6,001,215 |

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

```bash
source .venv/bin/activate
cd src/Collect_metrics
python collect.py
```

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

## Using Your Own Data

### Custom Workload Data

To use your own Snowset workload data:
1. Download from the [Snowset repository](https://github.com/resource-disaggregation/snowset)
2. Place CSV files in `src/Workloads/Snowset/`
3. Ensure filenames match those referenced in config files

### Custom Query Metrics

To collect metrics from your own benchmark databases:
1. Load your benchmark data into Databend
2. Run `src/Collect_metrics/collect.py` to collect query statistics
3. Metrics will be saved in `src/Collect_metrics/metrics_witho/output/`

Expected file format: `{query_type}-{database}-sql-metrics.json`

Each JSON file should contain an array of query metrics with:
- `query`: SQL query text
- `avg_cpu_time`: Average CPU time
- `avg_scan_bytes`: Average bytes scanned
- `avg_duration`: Average query duration
- `filter`, `join`, `agg`, `sort`: Operator counts

## Stopping Services

```bash
docker compose down       # Stop containers
docker compose down -v    # Stop and remove all data
```

## Project Structure

```
PBench/
├── src/
│   ├── Workloads/Snowset/              # Sample Snowset workload CSVs
│   ├── Collect_metrics/                # Metrics collection
│   │   └── metrics_witho/output/       # Sample query metrics
│   ├── PBench-tool/
│   │   ├── configs/                    # Configuration files
│   │   ├── output/plan/                # Generated optimization plans
│   │   └── run_pbench.py               # Main entry point
│   └── Baseline/                       # Baseline tools (CAB, Stitcher)
├── docker-compose.yml                  # Database services
├── prometheus.yml                      # Prometheus configuration
├── init_databases.sh                   # Database initialization
└── requirements.txt                    # Python dependencies
```

## References

- [PBench Paper](https://arxiv.org/abs/2506.16379) - VLDB'25 paper
- [Snowset Dataset](https://github.com/resource-disaggregation/snowset) - Real Snowflake query statistics
- [Databend Documentation](https://databend.rs/) - Database documentation
