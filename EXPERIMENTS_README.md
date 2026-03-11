# Reproducing All Experiments on EC2

This document covers the full process to reproduce every analysis notebook in `src/Collect_metrics/`:

| Notebook | Input Data | Scripts |
|---|---|---|
| `telemetry_noise_analysis.ipynb` | TPC-H SF20 + TPC-DS SF20 metrics (Databend & Firebolt) | `collect.py tpch20`, `collect.py tpcds20` |
| `telemetry_concurrency_analysis.ipynb` | TPC-H SF20 concurrency sweep (Firebolt) | `collect_concurrency.py` |
| `postgres_cpu_analysis.ipynb` | TPC-H SF20 CPU comparison (PostgreSQL & Firebolt) | `collect_postgres_cpu.py`, `collect.py tpch20 --firebolt` |
| `scan_comparison_analysis.ipynb` | Data-scanned comparison (Firebolt & Databend) | `collect_scan_comparison.py` |

---

## 0. EC2 Instance Requirements

- **Instance type**: r5.xlarge or larger (4 vCPUs, 32 GB RAM recommended — Databend alone needs ~16 GB)
- **Disk**: 100 GB+ gp3 (SF20 raw .tbl files ~22 GB, plus engine storage)
- **OS**: Ubuntu 22.04+
- **Software**: Docker, Docker Compose v2, Python 3.10+ (system Python 3.12 works), PostgreSQL 14+, git, gcc, make

```bash
# Install system packages
sudo apt update && sudo apt install -y docker.io docker-compose-v2 \
    postgresql postgresql-contrib \
    git gcc make bc curl

# Install Python venv support (package name varies by Ubuntu version)
sudo apt install -y python3-venv 2>/dev/null || sudo apt install -y python3.12-venv

# Allow current user to run Docker without sudo
sudo usermod -aG docker $USER
newgrp docker
```

---

## 1. Clone and Set Up Python Environment

```bash
git clone <repository-url> && cd PBench
git checkout query_pool

# Option A: uv (faster)
uv sync

# Option B: venv + pip
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Start Databend + Prometheus

```bash
docker compose up -d
docker compose ps   # wait ~15-30s for databend to become healthy
```

This starts:
- **Databend** on port `8000` (HTTP query API), `7070` (metrics)
- **Prometheus** on port `9091`, scraping Databend metrics every 1s
- **Init container** that creates all database schemas (tpch1g, tpcds1g, etc.)

Verify:
```bash
curl -s -u root: http://localhost:8000/v1/query/ \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['data'])"
```

---

## 3. Start MinIO (Required for Firebolt Data Loading)

MinIO provides S3-compatible storage that Firebolt-Core reads from via COPY.

```bash
# Install MinIO if not present
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio

# Create data dir (needs sudo since /home/ is root-owned)
sudo mkdir -p /home/shared
sudo chown $USER:$USER /home/shared

# Start MinIO
./minio server /home/shared &

# Install mc (MinIO client)
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
./mc alias set local http://localhost:9000 minioadmin minioadmin
```

**Port conflict note**: MinIO uses port 9000 by default, which can conflict with Jupyter kernel gateway. If running Jupyter, either use a different MinIO port (`--address :9002`) or stop Jupyter during data loading.

---

## 4. Generate TPC-H SF20 Data

```bash
cd databend-init

# Build dbgen (one-time)
./setup_tpch_dbgen.sh

# Generate SF20 .tbl files (~22 GB, takes a few minutes)
./load_tpch_dbgen.sh 20 tpch20g
# This generates files in databend-init/tpch-data/sf20/
# AND loads them into Databend (into tpch20g database)
```

After generation, verify the .tbl files exist:
```bash
ls -lh databend-init/tpch-data/sf20/*.tbl
# lineitem.tbl  ~15 GB
# orders.tbl    ~3.4 GB
# ...
```

---

## 5. Generate TPC-DS SF20 Data

```bash
cd databend-init

# Build dsdgen (one-time)
./setup_tpcds_dsdgen.sh

# Generate SF20 .dat files and load into Databend
./load_tpcds_dbgen.sh 20 tpcds20g
```

---

## 6. Load SF20 into All Three Engines

### 6a. Databend (TPC-H + TPC-DS)

If you used `load_tpch_dbgen.sh 20 tpch20g` above, TPC-H is already loaded.

Alternatively, if loading from MinIO (e.g., data was generated elsewhere):
```bash
# Upload .tbl files to MinIO first
./mc mb local/tpch/sf20 --ignore-existing
./mc cp databend-init/tpch-data/sf20/*.tbl local/tpch/sf20/

# Then use the MinIO-based loader
cd databend-init
bash load_sf20_databend.sh
```

This script creates `tpch20g` and `tpcds20g` databases in Databend, cloning schemas from `tpch1g`/`tpcds1g` (created by docker-compose init), then loads from MinIO stages.

Verify:
```bash
curl -s -u root: http://localhost:8000/v1/query/ \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM tpch20g.lineitem"}'
# Expected: 119,994,608 rows (SF20)
```

### 6b. PostgreSQL (TPC-H only — needed for `postgres_cpu_analysis.ipynb`)

```bash
cd databend-init

# Create a PostgreSQL role for the current user (one-time)
sudo -u postgres createuser --superuser $USER

# Requires .tbl files to already exist in tpch-data/sf20/
./load_tpch_postgres.sh 20 tpch20g
```

This creates the `tpch20g` database, schema, loads data via `\COPY`, and creates indexes + runs ANALYZE.

**Auth note**: By default uses Unix peer auth (no password). If your PostgreSQL requires password auth, set `PGUSER` and `PGPASSWORD` before running.

Verify:
```bash
psql -d tpch20g -c "SELECT COUNT(*) FROM lineitem;"
# Expected: ~119,994,608
```

### 6c. Firebolt-Core (TPC-H + TPC-DS — needed for all notebooks)

#### Start Firebolt-Core

```bash
cd databend-init/firebolt-core

# Pull the container image
chmod +x get-core.sh
./get-core.sh

# Create persistent data dir (must be owned by UID 1111)
mkdir -p ./firebolt-core-data
sudo chown -R 1111:1111 ./firebolt-core-data

# Start Firebolt
# IMPORTANT: must set PWD and COMPOSE_PROJECT_NAME explicitly
export COMPOSE_PROJECT_NAME=firebolt
sudo PWD=$(pwd) COMPOSE_PROJECT_NAME=firebolt docker compose up -d
```

Wait for health check:
```bash
curl -sf http://localhost:8122/ > /dev/null && echo "Firebolt ready"
```

#### Upload Data to MinIO for Firebolt

Firebolt-Core reads data via S3 (COPY FROM), pointing to MinIO:

```bash
# Upload TPC-H files
./mc mb local/tpch --ignore-existing
./mc mirror databend-init/tpch-data/sf20 local/tpch/sf20/

# Upload TPC-DS files
./mc mb local/tpcds --ignore-existing
./mc mirror databend-init/tpcds-data/sf20 local/tpcds/sf20/
```

**S3 endpoint in Firebolt config**: `databend-init/firebolt-core/config.json` has `"default_s3_endpoint_override": "http://host.docker.internal:9000"`. This works for Docker on Linux. If running directly on EC2 without Docker networking, you may need to change this to `http://172.17.0.1:9000` or `http://localhost:9000`.

#### Load TPC-H into Firebolt

```bash
cd databend-init
python load_tpch_firebolt.py 20 tpch20g localhost 3473
```

This creates the `tpch20g` database, schema, and runs `COPY ... FROM 's3://tpch/sf20/<table>.tbl'` with MinIO credentials.

#### Load TPC-DS into Firebolt

```bash
cd databend-init
python load_tpcds_firebolt.py 20 tpcds20g localhost 3473
```

Verify:
```bash
curl -s "http://localhost:3473/?database=tpch20g" -d "SELECT COUNT(*) FROM lineitem;"
# Expected: 119,994,608
```

---

## 7. Generate Query Pool JSON Files

The collection scripts read query pools from `src/Collect_metrics/metrics_witho/input/`. These should already exist in the repo. If missing or stale, regenerate:

```bash
cd src/Collect_metrics
python convert_queries.py
```

This reads raw SQL from `sql_queries/` (tpch/, tpcds/, job/, ceb/) and generates all `*-sql-input*.json` files.

Key files needed for SF20 experiments:
- `TPCH-tpch20g-sql-input.json` (Databend dialect, `@tpch20g` suffix)
- `TPCH-tpch20g-sql-input-standard.json` (PostgreSQL/Firebolt dialect)
- `tpcds_all-tpcds20g-sql-input.json` (Databend)
- `tpcds_all-tpcds20g-sql-input-postgres.json` (Firebolt uses this too)

---

## 8. Configure Environment

```bash
cd src/Collect_metrics
cp ../../.env.sample .env
```

Edit `.env`:
```
HOST=localhost
DATABEND_PORT=8000
PROMETHEUS_PORT=9091
WAIT_TIME=2
PG_HOST=
PG_PORT=5432
PG_USER=
PG_PASSWORD=
FIREBOLT_HOST=localhost
FIREBOLT_PORT=3473
```

---

## Experiment 1: Telemetry Noise Analysis

**Notebook**: `telemetry_noise_analysis.ipynb`

**Required data files** (in `metrics_witho/output/`):
- `TPCH-tpch20g-sql-metrics.json` (Databend)
- `TPCH-tpch20g-sql-metrics-firebolt.json` (Firebolt)
- `tpcds_all-tpcds20g-sql-metrics.json` (Databend)
- `tpcds_all-tpcds20g-sql-metrics-firebolt.json` (Firebolt)

### Step 1: Collect TPC-H SF20 on Databend

```bash
cd src/Collect_metrics
python collect.py tpch20 --repeat 3 --timeout 300
```

Output: `metrics_witho/output/TPCH-tpch20g-sql-metrics.json`

**How it works**: For each of the 22 TPC-H queries, runs `--repeat` iterations. Each iteration:
1. Sleeps 6s (wait for Prometheus scrape)
2. Reads Prometheus counters (CPU seconds, scan bytes)
3. Executes query via `databend-driver`
4. Sleeps 6s again
5. Reads Prometheus counters again, takes delta
6. Runs `EXPLAIN ANALYZE` to extract operator flags (filter/join/agg/sort)

Total time: ~30-45 min for 3 repeats (6+6=12s overhead per query).

### Step 2: Collect TPC-H SF20 on Firebolt

```bash
python collect.py tpch20 --firebolt --no-databend --repeat 3 --timeout 300
```

Output: `metrics_witho/output/TPCH-tpch20g-sql-metrics-firebolt.json`

**How it works**: POSTs SQL to `http://localhost:3473/?database=tpch20g&enable_subresult_cache=false`, extracts `Firebolt-Query-Id` from response header, then polls `information_schema.engine_query_history` for `cpu_usage_us`, `scanned_bytes`, `duration_us`.

### Step 3: Collect TPC-DS SF20 on Databend

```bash
python collect.py tpcds20 --repeat 3 --timeout 300
```

Output: `metrics_witho/output/tpcds_all-tpcds20g-sql-metrics.json`

### Step 4: Collect TPC-DS SF20 on Firebolt

```bash
python collect.py tpcds20 --firebolt --no-databend --repeat 3 --timeout 300
```

Output: `metrics_witho/output/tpcds_all-tpcds20g-sql-metrics-firebolt.json`

### Step 5: Run the Notebook

```bash
jupyter notebook telemetry_noise_analysis.ipynb
```

The notebook compares per-query telemetry variation across repeats for each engine, producing collision-pair analysis charts.

---

## Experiment 2: Concurrency Telemetry Analysis

**Notebook**: `telemetry_concurrency_analysis.ipynb`

**Required data file**: `metrics_witho/output/concurrency_telemetry_firebolt.json`

### Step 1: Collect Concurrency Data (Firebolt-only)

```bash
cd src/Collect_metrics
python collect_concurrency.py tpch20 --concurrency 1 2 4 --rounds 3 --timeout 120
```

Output: `metrics_witho/output/concurrency_telemetry_firebolt.json`

**How it works**: For each query, for each concurrency level C, fires C copies of the same query simultaneously using `ThreadPoolExecutor`. Records per-copy `cpu_ms`, `scanned_bytes`, `duration_ms` from Firebolt's `engine_query_history`.

**Important**: Concurrency C=8 is known to crash Firebolt-Core on certain queries (Q7, Q9, Q17). The script has auto-restart logic — if Firebolt becomes unhealthy after 3 consecutive failures, it runs `docker compose up -d --wait core` to restart the container.

Env vars for auto-restart:
```bash
export FIREBOLT_COMPOSE_DIR=/path/to/databend-init/firebolt-core
export COMPOSE_PROJECT_NAME=firebolt
```

### Step 2: Run the Notebook

```bash
jupyter notebook telemetry_concurrency_analysis.ipynb
```

---

## Experiment 3: PostgreSQL CPU Analysis

**Notebook**: `postgres_cpu_analysis.ipynb`

**Required data files**:
- `metrics_witho/output/TPCH-tpch20g-sql-metrics-postgres-cpu.json`
- `metrics_witho/output/TPCH-tpch20g-sql-metrics-firebolt.json` (from Experiment 1, Step 2)

### Step 1: Collect PostgreSQL CPU Data

```bash
cd src/Collect_metrics
python collect_postgres_cpu.py --database tpch20g --rounds 3 --timeout 120
```

Output: `metrics_witho/output/TPCH-tpch20g-sql-metrics-postgres-cpu.json`

**How it works**: For each TPC-H query:
1. Opens a PostgreSQL connection, gets the backend PID
2. Reads `/proc/<pid>/stat` for user + system CPU ticks (before)
3. Runs `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for execution time + buffer stats
4. Reads `/proc/<pid>/stat` again (after), computes delta

**Requirement**: PostgreSQL must run locally (not in Docker) so that `/proc/<pid>/stat` is accessible. The script warns but continues if proc stats are unavailable.

### Step 2: Run the Notebook

```bash
jupyter notebook postgres_cpu_analysis.ipynb
```

Compares OS-level `/proc` CPU time vs `EXPLAIN ANALYZE` execution time vs Firebolt CPU.

---

## Experiment 4: Scan Comparison (Firebolt vs Databend)

**Notebook**: `scan_comparison_analysis.ipynb`

**Required data file**: `metrics_witho/output/scan_comparison_firebolt_databend.json`

### Step 1: Collect Scan Comparison Data

```bash
cd src/Collect_metrics
python collect_scan_comparison.py --rounds 3 --timeout 300
```

Output: `metrics_witho/output/scan_comparison_firebolt_databend.json`

**How it works**: Two sub-experiments on `tpch20g`:

1. **Full table scans**: Runs `SELECT SUM(column) FROM table` on `supplier`, `orders`, `lineitem` on both engines.
2. **Selectivity sweep**: Runs `SELECT SUM(l_extendedprice) FROM lineitem WHERE l_shipdate <= DATE '...'` at 1%, 5%, 10%, 25%, 50%, 75%, 100% selectivity.

For Databend: uses Prometheus counter deltas (same approach as `collect.py` — 6s sleeps before/after for scrape alignment). For Firebolt: uses `engine_query_history` stats.

**Important gotchas learned during development**:
- Databend HTTP API: the `database` field in JSON body is ignored — you must use `"session": {"database": "tpch20g"}`.
- Databend: `COUNT(*)` is answered from metadata without scanning data (0 bytes, ~3ms). The script uses `SUM(column)` aggregates to force real scans.
- Scanned bytes differ ~25x between engines because of different metric semantics: Firebolt reports logical bytes of projected columns only; Databend's Prometheus counter reports total segment bytes including all columns.

### Step 2: Run the Notebook

```bash
jupyter notebook scan_comparison_analysis.ipynb
```

---

## Auto-Resume Behavior

All collection scripts support auto-resume:
- `collect.py`: resumes from `min(record_count)` across enabled backends
- `collect_concurrency.py`: skips `(query_idx, concurrency, round, copy)` tuples already in output JSON
- `collect_scan_comparison.py`: skips `(experiment, table, selectivity, backend, round)` tuples already in output JSON
- `collect_postgres_cpu.py`: skips `(query_idx, round)` tuples already in output JSON

If a run is interrupted, simply re-run the same command and it picks up where it left off.

---

## Quick Checklist

Before running any experiment, verify:

```bash
# Databend HTTP API
curl -sf -u root: http://localhost:8000/v1/query/ \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT 1"}' && echo "Databend OK"

# Prometheus
curl -sf http://localhost:9091/api/v1/status/buildinfo > /dev/null && echo "Prometheus OK"

# Firebolt-Core
curl -sf http://localhost:3473/ -d "SELECT 1" && echo "Firebolt OK"

# PostgreSQL
psql -d tpch20g -c "SELECT 1" && echo "PostgreSQL OK"

# MinIO (only needed during data loading)
curl -sf http://localhost:9000/minio/health/live && echo "MinIO OK"
```

---

## Port Reference

| Service | Port | Protocol |
|---|---|---|
| Databend HTTP query API | 8000 | HTTP |
| Databend Metrics | 7070 | HTTP (Prometheus scrape) |
| Prometheus | 9091 | HTTP |
| Firebolt-Core query | 3473 | HTTP |
| Firebolt-Core health | 8122 | HTTP |
| PostgreSQL | 5432 | TCP |
| MinIO S3 | 9000 | HTTP |

---

## Disk Space Budget

| Item | Approximate Size |
|---|---|
| TPC-H SF20 .tbl files | ~22 GB |
| TPC-DS SF20 .dat files | ~21 GB |
| Databend storage (TPC-H+DS SF20) | ~20 GB |
| PostgreSQL storage (TPC-H SF20) | ~25 GB |
| Firebolt storage (TPC-H+DS SF20) | ~20 GB |
| Docker images | ~5 GB |
| **Total** | **~110 GB** |

You can delete the raw `.tbl`/`.dat` files after loading into all engines to reclaim ~43 GB.

---

## Experiment 5: ILP Workload Recovery (Ground Truth Matching)

**Goal**: Run a known mix of TPC-H queries against Databend, collect aggregate telemetry, then feed that telemetry to PBench's ILP solver to see if it can reconstruct the original query mix.

This tests PBench's core capability: given only aggregate statistics (total CPU, total scan bytes, average duration, operator ratios), can it reverse-engineer which queries produced them?

The workload can be run sequentially (simplest, most deterministic telemetry) or concurrently (more realistic, but noisier metrics).

### Prerequisites

- Databend running with TPC-H SF20 loaded into `tpch20g`
- Prometheus scraping Databend metrics on port 9091
- SF20 query metrics collected: `metrics_witho/output/TPCH-tpch20g-sql-metrics.json`

If you don't have SF20 metrics yet:
```bash
cd src/Collect_metrics
python collect.py tpch20 --repeat 3 --timeout 300
```

### Step 1: Define a Secret Query Mix

There are three ways to define the workload that PBench will try to recover.

#### Step 1.0: Manual mix JSON (simplest)

Create a file in `experiments/mixes/` with a query mix. Each key is a 0-based query
index (Q1=0, Q2=1, ..., Q22=21) and the value is the execution count.

Example mixes of increasing difficulty:

**Easy** — few distinct queries, different operator signatures:
```json
{
    "name": "easy",
    "description": "3 distinct queries, clear signatures",
    "mix": {"0": 5, "8": 3, "5": 2},
    "comment": "Q1(filter+agg+sort) x5, Q9(all ops) x3, Q6(filter+agg only) x2"
}
```

**Medium** — more queries, some with similar profiles:
```json
{
    "name": "medium",
    "description": "5 distinct queries including similar-profile pairs",
    "mix": {"0": 3, "8": 2, "12": 1, "17": 2, "20": 1},
    "comment": "Q1 x3, Q9 x2, Q13 x1, Q18 x2, Q21 x1 — Q9/Q18 both have all operators"
}
```

**Hard** — many queries with overlapping signatures:
```json
{
    "name": "hard",
    "description": "8 distinct queries, overlapping operator profiles",
    "mix": {"0": 2, "2": 1, "4": 2, "6": 1, "8": 3, "9": 1, "14": 2, "17": 1},
    "comment": "Most queries have all 4 operators — ILP must rely on CPU/scan/duration differences"
}
```

#### Step 1.1: Groundtruth sequential run

Use the groundtruth runner (`groundtruth/run_bench.py`) to execute all 22 TPC-H queries
sequentially against Databend, collecting per-query Prometheus telemetry. Then aggregate
the events into a single time-slot for ILP recovery.

```bash
# Create a config for SF20 sequential run
cat > groundtruth/configs/tpch20g_seq.yml << 'EOF'
run:
  run_id: tpch20g-seq
  output_root: groundtruth/output

engine:
  type: databend
  host: localhost
  port: 8000
  default_database: default
  prometheus_host: localhost
  prometheus_port: 9091
  prometheus_scrape_wait_s: 2.0

workload:
  benchmark: tpch
  query_pool_path: src/Collect_metrics/metrics_witho/input/TPCH-tpch20g-sql-input.json

execution:
  mode: sequential
  concurrency: 1
  timeout_s: 600
  seed: 42
  selection_mode: as_is
  max_queries: 22
EOF

# Run all 22 queries sequentially
python groundtruth/run_bench.py --config groundtruth/configs/tpch20g_seq.yml
```

This produces `groundtruth/output/tpch20g-seq/output_events.jsonl` — one event per
query with Prometheus-captured CPU, scan bytes, duration, and operator flags. To convert
this into a mix JSON for the recovery script:

```bash
python experiments/events_to_mix.py \
    --events groundtruth/output/tpch20g-seq/output_events.jsonl \
    --output experiments/mixes/groundtruth_seq.json
```

The advantage of this approach is that every query gets real observed telemetry from
Prometheus, rather than relying on pre-collected metrics averages.

#### Step 1.2: CAB baseline stream (first 5 minutes)

The `groundtruth/cab/` directory contains 5 pre-generated CAB workload streams, each
spanning ~24 hours of simulated Snowflake traffic. The `query_id` field in each entry
maps to a TPC-H query number (1-indexed). Each entry also carries `arguments` for
parameterized queries and a `start` timestamp in milliseconds.

To extract a 5-minute window from a CAB stream and convert it to a mix:

```bash
python experiments/cab_to_mix.py \
    --stream groundtruth/cab/query_stream_3.json \
    --minutes 5 \
    --output experiments/mixes/cab_stream3_5min.json
```

Stream 3's first 5 minutes contains a manageable 13 queries across 11 distinct TPC-H
templates:

| Stream | First 5 min | Distinct | Example distribution |
|--------|-------------|----------|---------------------|
| 0 | 9 queries | 7 | Q12 x3, Q1/Q2/Q4/Q17/Q18/Q19 x1 each |
| 1 | 101 queries | 19 | Heavy mix, all templates represented |
| 2 | 51 queries | 16 | Q4 x7, Q2 x6, Q3/Q18/Q22 x5 each |
| 3 | 13 queries | 11 | Q4/Q19 x2, 9 others x1 each |
| 4 | 28 queries | 13 | Q18 x5, Q1/Q15/Q16 x3 each |

**Note**: CAB streams use query IDs 1-22 (1-indexed TPC-H numbers). The `cab_to_mix.py`
script converts these to 0-indexed for the ILP solver. Some CAB query IDs (e.g., 23)
may not map to standard TPC-H — these are skipped with a warning.

### Step 2: Run the Query Mix and Collect Telemetry

**Option A — Sequential** (simplest, most deterministic — recommended for validation):

```bash
cd PBench
source .venv/bin/activate
python experiments/run_ground_truth.py \
    --mix experiments/mixes/easy.json \
    --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
    --database tpch20g \
    --concurrency 1 \
    --output experiments/results/
```

Sequential mode runs one query at a time, giving the cleanest Prometheus deltas (no
overlapping counters) and the most deterministic telemetry. Best for validating the
ILP solver works correctly.

**Option B — Concurrent** (more realistic, noisier):

```bash
python experiments/run_ground_truth.py \
    --mix experiments/mixes/medium.json \
    --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
    --database tpch20g \
    --concurrency 4 \
    --output experiments/results/
```

The script:
1. Reads the secret query mix
2. Executes all queries against Databend (sequentially or concurrently)
3. Collects Prometheus CPU/scan deltas and client-side durations
4. Computes aggregate telemetry: total CPU, total scan bytes, average duration, operator ratios
5. Saves observed telemetry to `experiments/results/<name>_telemetry.json`

### Step 3: Run PBench ILP Recovery

Run `experiments/recover_mix.py`:

```bash
python experiments/recover_mix.py \
    --telemetry experiments/results/<name>_telemetry.json \
    --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
    --secret experiments/ground_truth_mix.json \
    --count-limit 30
```

The script:
1. Loads the observed telemetry (what PBench sees)
2. Builds a candidate pool from the SF20 metrics file
3. Runs PBench's ILP solver (`solve_integer_linear_programming_cycle`)
4. Compares the ILP solution against the secret ground truth
5. Prints a comparison table and accuracy metrics

### Expected Output

```
======================================================================
PBENCH's GUESS vs GROUND TRUTH
======================================================================

Query     Truth  Guess  Match?   cpu(s)   scan(GB)   dur(s)
-----------------------------------------------------------
Q1            5      5       ✓    51.00     10.648    3.711
Q6            2      2       ✓    10.00      0.049    0.856
Q9            3      3       ✓   139.00      8.426   10.910

Metric               Truth      Guess    Error
------------------------------------------------
CPU (s)             702.00     702.00     0.0%
Scan (GB)            53.48      53.48     0.0%
Count                   10         10

Exact: 3/3, Close(±1): 0, Objective: 0.0000
```

### Step 4: Batch Evaluation

To run all three difficulty levels and compare:

```bash
for mix in easy medium hard; do
    python experiments/run_ground_truth.py \
        --mix experiments/mixes/${mix}.json \
        --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
        --database tpch20g \
        --concurrency 4 \
        --output experiments/results/

    python experiments/recover_mix.py \
        --telemetry experiments/results/${mix}_telemetry.json \
        --metrics src/Collect_metrics/metrics_witho/output/TPCH-tpch20g-sql-metrics.json \
        --secret experiments/mixes/${mix}.json \
        --count-limit 30
done
```

### Interpreting Results

Key metrics to evaluate:

| Metric | Meaning |
|---|---|
| **Exact matches** | Number of queries where ILP guessed the exact count |
| **Close matches (±1)** | Queries where the count is off by 1 |
| **Objective value** | ILP minimization objective — 0 means perfect fit |
| **CPU/Scan error %** | How close the reconstructed aggregate telemetry is to observed |

**Why SF20 matters**: SF1 metrics have poor granularity — Prometheus reports CPU in ~1s increments, making many queries indistinguishable. SF20 queries run 10-100x longer, giving much better metric separation. Compare Q6 at SF1 (cpu=1s, dur=0.012s) vs SF20 (cpu=10s, dur=0.856s).

**When ILP struggles**: The solver has difficulty when:
- Multiple queries have nearly identical metric profiles (e.g., Q7 and Q8 at SF20: both ~11s CPU, ~1s duration, all operators)
- The count limit is too loose — the solver can split one Q9 execution into many small queries that sum to the same CPU
- Operator ratios are uniform (most SF20 queries have all 4 operators = 1)

**When ILP excels**: Recovery is most accurate when:
- Queries span a wide range of CPU and scan values
- Some queries have distinct operator signatures (e.g., Q6 has no join/sort)
- The count hint (`initial_count`) is close to the actual number of executions
