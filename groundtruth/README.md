# Groundtruth

Groundtruth pipeline with two entrypoints:

1. `run_bench.py`: runs a config-defined query pool and writes canonical event JSONL.
2. `create_aggregate_trace.py`: converts canonical events to PBench-compatible aggregate CSV (`snowset` or `redset`).

## Scope

- Current reference path is Databend.
- Default TPCH sequential config runs all 22 TPCH queries (`max_queries: 22`).
- Query identity uses Databend `last_query_id()`.
- Operator flags/counts come from `EXPLAIN ANALYZE` plan parsing.

## Reproducibility Preconditions

Before running groundtruth scripts, make sure all of the following are true:

1. Environment is set up as in root [`HOW_TO_RUN.md`](../HOW_TO_RUN.md).
2. Databend is running and reachable from this machine.
3. Benchmark schemas/data are loaded (TPCH/TPCDS/IMDB as needed for your query pool).
4. Query-pool JSON files referenced by config exist under `src/Collect_metrics/metrics_witho/input/`.
5. If needed, regenerate query pools using [`src/Collect_metrics/README.md`](../src/Collect_metrics/README.md) (`convert_queries.py`).
6. If using aggregate scripts directly, understand window semantics from [`src/Workloads/README.md`](../src/Workloads/README.md).
7. Endpoint/port values in config match your deployment (defaults assume docker-compose style setup).

## Preflight Checks

Run these checks before a measured run:

```bash
# 1) Databend HTTP API reachable
curl -sSf http://<HOST>:<DATABEND_PORT>/v1/query > /dev/null

# 2) Prometheus reachable (for sequential per-query cpu/scan)
curl -sSf http://<HOST>:<PROMETHEUS_PORT>/api/v1/status/buildinfo > /dev/null

# 3) Benchmark DB exists and has data
# (example via Databend SQL client of your choice)
# SELECT COUNT(*) FROM tpch1g.lineitem;
```

## Quick Start

```bash
python groundtruth/run_bench.py --config groundtruth/configs/databend_tpch_seq.yml --dry-run
python groundtruth/run_bench.py --config groundtruth/configs/databend_tpch_seq.yml

python groundtruth/create_aggregate_trace.py \
  --input-events groundtruth/output/demo-databend-seq/events.jsonl \
  --format snowset \
  --duration-seconds 120 \
  --bucket-seconds 60 \
  --subbucket-seconds 10 \
  --output groundtruth/output/demo-databend-seq/workload_snowset_120s_60s_10s.csv

python groundtruth/create_aggregate_trace.py \
  --input-events groundtruth/output/demo-databend-seq/events.jsonl \
  --format redset \
  --duration-seconds 120 \
  --bucket-seconds 60 \
  --subbucket-seconds 10 \
  --output groundtruth/output/demo-databend-seq/workload_redset_120s_60s_10s.csv
```

## Consume In PBench

After generating aggregate CSVs, place them where `src/PBench-tool` expects workloads and create a matching config directory.

### 1) Copy workload CSV

Snowset-style:

```bash
cp groundtruth/output/demo-databend-seq/workload_snowset_120s_60s_10s.csv \
  src/Workloads/Snowset/workload_gt_tpch_seq_120s.csv
```

Redset-style:

```bash
cp groundtruth/output/demo-databend-seq/workload_redset_120s_60s_10s.csv \
  src/Workloads/Redset/workload_gt_tpch_seq_120s.csv
```

### 2) Ensure query metrics JSON exists for selected query pool/database

For TPCH + `tpch1g`, `run_pbench.py` expects:

```text
src/Collect_metrics/metrics_witho/output/TPCH-tpch1g-sql-metrics.json
```

If missing, generate with:

```bash
cd src/Collect_metrics
python collect.py tpch --databend --repeat 3 --prometheus-wait-seconds 2.0
```

### 3) Create a PBench config directory

Example (Snowset workload, TPCH-only synthesis):

```bash
mkdir -p src/PBench-tool/configs/groundtruth/workload_gt_tpch_seq_120s
```

Create file `src/PBench-tool/configs/groundtruth/workload_gt_tpch_seq_120s/llm-queryset-witho.yml`:

```yaml
workload_path: ../../src/Workloads/Snowset/workload_gt_tpch_seq_120s.csv
workload_name: workload_gt_tpch_seq_120s
host: localhost
databend_port: 8000
prometheus_port: 9091
count_limit: 1000
time_limit: 270
use_operator: 1
wait: 8
interval: 30
query:
- TPCH
db:
- tpch1g
op_scale: 100
initial_count: 10
use_duration: 0
```

Notes:

1. `workload_path` is resolved from `src/PBench-tool`.
2. `query` + `db` entries must align with available metrics JSON names.
3. For Redset CSV, set `workload_path` to `../../src/Workloads/Redset/<file>.csv`.

### 4) Run PBench with that config directory

```bash
cd src/PBench-tool
python run_pbench.py --config-dir configs/groundtruth
```

Outputs are written under:

```text
src/PBench-tool/output/plan/<workload_name>/
src/PBench-tool/output/sa_plan/<workload_name>/
src/PBench-tool/output/replay_ta/<workload_name>/
```

If you want details on config semantics and output files, see [`src/PBench-tool/README.md`](../src/PBench-tool/README.md).

## Config Notes

- `engine.host` / `engine.port`: Databend endpoint.
- `engine.prometheus_host` / `engine.prometheus_port`: Prometheus endpoint.
- `workload.query_pool_path`: input query pool JSON (`query@database` format).
- `execution.max_queries`: limits how many pool entries are executed.
- `execution.selection_mode`:
  - `as_is`: keep pool order.
  - `shuffle`: reproducible shuffle via `execution.seed`.
  - `sample_with_replacement`: random sampling with replacement.

## Metric Semantics

- Sequential mode (`mode=sequential`, `concurrency=1`):
  - `cpu_ms` and `scan_bytes` are populated from Prometheus delta and marked `*_prov=proxy`.
- Concurrent mode:
  - per-query Prometheus attribution is disabled by design; `cpu_ms`/`scan_bytes` are `null` with `*_prov=missing`.
- `memory_bytes` is currently unavailable in Databend CE path and remains `missing`.

## Choosing Aggregation Windows

For short runs (for example one TPCH pass), avoid coarse `300s/30s` defaults.

Rule of thumb:

1. choose `bucket_seconds` so each bucket has roughly `5-20` queries,
2. choose `subbucket_seconds` as about `1/6` to `1/10` of bucket size.

For a ~100-120 second TPCH run:

- `duration_seconds=120`
- `bucket_seconds=60`
- `subbucket_seconds=10`

## Common Pitfalls

1. Only a few events written: check `execution.max_queries` in config.
2. SQL parse errors: use the Databend-compatible pool (`*-sql-input-standard.json`).
3. Empty/mostly-zero aggregates: bucket/window too coarse for short runs.
4. Prometheus errors: endpoint/port mismatch (common default is `9091` in this repo).
5. Query pools missing/stale: regenerate with `src/Collect_metrics/convert_queries.py`.
6. `run_pbench.py` fails with missing metrics JSON: verify `query`/`db` pairs map to existing files in `src/Collect_metrics/metrics_witho/output/`.
