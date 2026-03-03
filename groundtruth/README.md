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

- The Databend adapter is implemented first and is designed to support sequential and concurrent dispatch.
- Query identity linking is done via an embedded request token (`/* gt:<id> */`) and best-effort history-table lookup.
- If a metric is unavailable from Databend history, its provenance is marked as `missing` or `proxy`.
