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

### Optional LLM prerequisites

If you want to enable LLM-assisted query generation (additional pass in ILP):

1. Put at least one API key in:
   - `src/PBench-tool/LLM_tools/input/keys.txt`
2. Ensure base metrics exist for the configured query pools:
   - `src/Collect_metrics/metrics_witho/output/{query_set}-{database}-sql-metrics.json`
3. (Optional) Configure OpenAI-compatible endpoint/model via env or config (see below).

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

## LLM Pass Controls

By default, LLM generation is disabled.

- Default behavior:
  - `llm_times = 0` (no LLM pass)
- Enable LLM pass:
  - set `llm_times` in YAML config, or
  - set `LLM_TIMES` env var

Resolution for `llm_times`:
- `config["llm_times"]` takes precedence
- fallback: `LLM_TIMES`
- fallback default: `0`

### Run without LLM pass (default)

No change required. Equivalent explicit config:

```yaml
llm_times: 0
```

### Run with LLM pass

Example YAML snippet:

```yaml
llm_times: 1
llm_model: gpt-4o
llm_query_timeout_secs: 120
llm_result_timeout_secs: 120
```

Or via env:

```bash
export LLM_TIMES=1
export OPENAI_MODEL=gpt-4o
export LLM_QUERY_TIMEOUT_SECS=120
export LLM_RESULT_TIMEOUT_SECS=120
```

### Model and endpoint selection

Model resolution:

- `config["llm_model"]` (if present)
- else `OPENAI_MODEL` env var
- else default `gpt-4o`

LLM query timeout resolution (used for `EXPLAIN ANALYZE` replay in LLM generation):

- `config["llm_query_timeout_secs"]` (if present)
- else `LLM_QUERY_TIMEOUT_SECS` env var
- else default `120`

LLM Databend HTTP result timeout resolution:

- `config["llm_result_timeout_secs"]` (if present)
- else `LLM_RESULT_TIMEOUT_SECS` env var
- else default `120`

This value is passed as Databend session setting `http_handler_result_timeout_secs`.

OpenAI-compatible endpoint:

```bash
export OPENAI_BASE_URL="https://your-openai-compatible-endpoint/v1"
```

## LLM Metrics Files

When LLM pass is enabled, generated query metrics are written to separate files:

- `src/Collect_metrics/metrics_witho/output/{query_set}-{database}-sql-metrics-llm.json`

The synthesis pipeline automatically reads both:

- base metrics: `...-sql-metrics.json`
- LLM metrics: `...-sql-metrics-llm.json` (if present)

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
