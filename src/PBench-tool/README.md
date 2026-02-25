# PBench-tool

This folder contains the workload synthesis pipeline used by `run_pbench.py`:

1. ILP workload fitting (`linearprogram_option.py`)
2. Slot/subslot assignment via simulated annealing (`simulatedannealing.py`)
3. Replay + metric capture (`replay_ta.py`)

## Prerequisites

Before running `run_pbench.py`, make sure:

1. Aggregate workload CSV exists (`src/Workloads/.../*.csv`).
2. Query-pool metrics JSON exists for the query set + database used in config.
   - Required path pattern:
     - `src/Collect_metrics/metrics_witho/output/{query_set}-{database}-sql-metrics.json`
   - Example for redbench on imdb:
     - `src/Collect_metrics/metrics_witho/output/redbench-imdb-sql-metrics.json`
3. Databend + Prometheus are running and reachable from config host/ports.
   - In this repo's `docker-compose.yml`, Prometheus is exposed on host port `9091`.

## Collect Query Metrics First

Example (from repo root):

```bash
python src/Collect_metrics/collect.py redbench --databend --repeat 3 --prometheus-wait-seconds 2.0
```

This generates:

```text
src/Collect_metrics/metrics_witho/output/redbench-imdb-sql-metrics.json
```

## Run PBench

`run_pbench.py` expects a config directory that contains one or more `.yml` files.

### Example: redbench configs

From `src/PBench-tool`:

```bash
python run_pbench.py --config-dir configs/redbench
```

### Example: redset configs

```bash
python run_pbench.py --config-dir configs/redset
```

### Example: snowset configs

```bash
python run_pbench.py --config-dir configs/snowset
```

## Config Organization

There are multiple config trees for different workload/query-pool combinations:

- `configs/redbench/...`
- `configs/redset/...`
- `configs/snowset/...`

Use `--config-dir` to select which set to run.

## Output Structure

`run_pbench.py` writes into `src/PBench-tool/output/`:

- `output/plan/<workload_name>/<queryset>-plan.json`
  - ILP result (query counts per 5-minute slot)
- `output/sa_plan/<workload_name>/<queryset>-plan2.json`
  - SA result (queries assigned to 30-second subslots)
- `output/replay_ta/<workload_name>/<queryset>-results.json`
  - Replay measurements (CPU/scan interval and totals, duration)

For execution/replay use, `sa_plan` is the final synthesized schedule.

## Convert SA Plan to Flat Trace CSV

Use `convert_sa_plan_to_trace.py` to convert one or multiple SA plans into:

```text
arrival_timestamp,query_type,sql,read_tables,write_table
```

### Single plan example

```bash
python convert_sa_plan_to_trace.py \
  --sa-plan output/sa_plan/workload1h-5m-30s_186_8_1/redbench-plan2.json \
  --start 2024-03-01T07:51:14Z \
  --output output/redbench_186_8_1_trace.csv
```

### Concatenate multiple plans sequentially

```bash
python convert_sa_plan_to_trace.py \
  --sa-plan \
    output/sa_plan/workload1h-5m-30s_186_8_1/redbench-plan2.json \
    output/sa_plan/workload1h-5m-30s_186_8_2/redbench-plan2.json \
    output/sa_plan/workload1h-5m-30s_186_8_3/redbench-plan2.json \
    output/sa_plan/workload1h-5m-30s_186_8_4/redbench-plan2.json \
    output/sa_plan/workload1h-5m-30s_186_8_5/redbench-plan2.json \
  --start 2024-03-01T07:00:00Z \
  --output output/redbench_186_8_concat_trace.csv
```

### Converter assumptions

1. `run_pbench.py` has already produced `output/sa_plan/...`.
2. `--start` is required because SA plan JSON has no absolute timestamps.
3. Slot/subslot defaults are 5 minutes / 30 seconds.
4. Timestamps are synthetic (derived from plan position), not original trace timestamps.
5. SQL is normalized to one line in output.
6. Table extraction is regex-based (good for common patterns, not a full SQL parser).
