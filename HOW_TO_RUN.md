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
python3.10 -m venv .venv
source .venv/bin/activate  # On Linux/Mac, or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 3. Start Databend Database and Initialize Databases

```bash
docker compose up -d
```

Wait for containers to be healthy (~15-30 seconds):
```bash
docker compose ps
```

The required database should be automatically created based on the following script.

```bash
./init_databases.sh
```

The script creates the required TPCH, TPCDS, IMDB, and LLM databases.

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

```bash
source .venv/bin/activate
cd src/Baseline
python do_baseline.py
```

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
