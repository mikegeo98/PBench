# Groundtruth

Minimal groundtruth pipeline with two entrypoints:

1. `run_bench.py`: executes a config-defined query pool and writes canonical event JSONL.
2. `create_aggregate_trace.py`: converts canonical events into PBench-compatible aggregate CSV (`snowset` or `redset`).

## Quick Start

```bash
python groundtruth/run_bench.py --config groundtruth/configs/databend_tpch_seq.yml --dry-run
python groundtruth/run_bench.py --config groundtruth/configs/databend_tpch_seq.yml
python groundtruth/create_aggregate_trace.py \
  --input-events groundtruth/output/demo-databend-seq/events.jsonl \
  --format snowset \
  --output groundtruth/output/demo-databend-seq/workload_snowset.csv
```

## Notes

- The Databend adapter is implemented first.
- Sequential mode (`execution.mode=sequential`, `execution.concurrency=1`) supports per-query Prometheus deltas for `cpu_ms` and `scan_bytes` when `engine.prometheus_port` is configured.
- Existing `Collect_metrics` defaults typically use Prometheus on port `9091`; use that unless your deployment differs.
- Concurrent mode is still supported, but Prometheus delta collection is disabled for concurrent runs.
- `engine.enable_system_query_log_lookup` defaults to `false` and should stay disabled on Databend Community (avoids extra `SHOW TABLES FROM system` probes).
- Query identity linking is done via an embedded request token (`/* gt:<id> */`) and best-effort history-table lookup.
- Operator flags/counts are extracted from `EXPLAIN` plan text (fallback: SQL text proxy).
- If a metric is unavailable from Databend history/Prometheus, its provenance is marked as `missing` or `proxy`.
