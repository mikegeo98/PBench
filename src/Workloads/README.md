# Workload Aggregate Scripts

This folder contains the Snowset aggregate rebuild script and its regression test:

- `src/Workloads/create_snowset_aggregates.py`
- `src/Workloads/test_create_snowset_aggregates.py`

The script rebuilds aggregate workload CSVs (5-minute slots with 30-second sub-intervals)
from raw Snowset parquet inputs.

## Database IDs (from existing aggregate CSVs)

These are the `databaseid` values present in the current aggregate files:

| Aggregate file | databaseid | start |
|---|---:|---|
| `workload1h-5m-30s_1.csv` | `6962091905597855564` | `2018-02-22 08:35:00` |
| `workload1h-5m-30s_2.csv` | `7834499828131635729` | `2018-02-22 09:05:00` |
| `workload1h-5m-30s_3.csv` | `5839665686231280450` | `2018-02-22 08:00:00` |
| `workload1h-5m-30s_4.csv` | `343722965207479532` | `2018-02-22 20:30:00` |
| `workload1h-5m-30s_5.csv` | `343722965207479532` | `2018-02-22 22:00:00` |

## Rebuild Snowset Aggregate Files

From repository root:

```bash
python src/Workloads/create_snowset_aggregates.py \
  --snowset-main-parquet original-workload-files/Snowset/snowset-main.parquet \
  --ts-explosion-parquet original-workload-files/Snowset/ts-explosion.parquet \
  --database-id 6962091905597855564 \
  --start 2018-02-22T08:35:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --output src/Workloads/Snowset/workload1h-5m-30s_1.csv

python src/Workloads/create_snowset_aggregates.py \
  --snowset-main-parquet original-workload-files/Snowset/snowset-main.parquet \
  --ts-explosion-parquet original-workload-files/Snowset/ts-explosion.parquet \
  --database-id 7834499828131635729 \
  --start 2018-02-22T09:05:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --output src/Workloads/Snowset/workload1h-5m-30s_2.csv

python src/Workloads/create_snowset_aggregates.py \
  --snowset-main-parquet original-workload-files/Snowset/snowset-main.parquet \
  --ts-explosion-parquet original-workload-files/Snowset/ts-explosion.parquet \
  --database-id 5839665686231280450 \
  --start 2018-02-22T08:00:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --output src/Workloads/Snowset/workload1h-5m-30s_3.csv

python src/Workloads/create_snowset_aggregates.py \
  --snowset-main-parquet original-workload-files/Snowset/snowset-main.parquet \
  --ts-explosion-parquet original-workload-files/Snowset/ts-explosion.parquet \
  --database-id 343722965207479532 \
  --start 2018-02-22T20:30:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --output src/Workloads/Snowset/workload1h-5m-30s_4.csv

python src/Workloads/create_snowset_aggregates.py \
  --snowset-main-parquet original-workload-files/Snowset/snowset-main.parquet \
  --ts-explosion-parquet original-workload-files/Snowset/ts-explosion.parquet \
  --database-id 343722965207479532 \
  --start 2018-02-22T22:00:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --output src/Workloads/Snowset/workload1h-5m-30s_5.csv
```

## Run Tests

Run regression tests that rebuild from parquet and compare with existing aggregate files.
From repository root:

```bash
python -m unittest -q src.Workloads.test_create_snowset_aggregates
```

## Redset Aggregates

### Files

- `src/Workloads/create_redset_aggregates.py`
- `src/Workloads/test_create_redset_aggregates.py`

Input data:

- `original-workload-files/Redset/full.parquet`

### Example command

```bash
python src/Workloads/create_redset_aggregates.py \
  --redset-parquet original-workload-files/Redset/full.parquet \
  --instance-id 0 \
  --database-id 0 \
  --start 2024-03-01T01:00:00Z \
  --duration-seconds 3600 \
  --bucket-seconds 300 \
  --subbucket-seconds 30 \
  --seed 42 \
  --filter-mode proxy \
  --sort-mode deterministic \
  --output src/Workloads/Redset/workload1h-5m-30s_db0.csv
```

If `--start` is omitted, the script automatically uses the first
`arrival_timestamp` for the selected `instance_id` + `database_id`.

### Mapping to aggregate fields

| Aggregate column | Redset source | Mapping |
|---|---|---|
| `databaseid` | `instance_id`, `database_id` | composite key `<instance_id>:<database_id>` |
| `qminute` | `arrival_timestamp` | slot start (5-minute bucket) |
| `cputime_sum` | `execution_duration_ms` | proxy: overlap-weighted execution seconds |
| `scanbytes_sum` | `mbytes_scanned` | overlap-weighted sum, MB -> GiB |
| `avg_durationtime` | `compile_duration_ms + queue_duration_ms + execution_duration_ms` | average total duration (seconds) over active queries in slot |
| `join` | `num_joins` | average of `(num_joins > 0)` |
| `agg` | `num_aggregations` | average of `(num_aggregations > 0)` |
| `cputime_interval` | execution proxy + overlap | overlap-weighted per subbucket |
| `scanbytes_interval` | scan + overlap | overlap-weighted per subbucket |
| `duration_interval` | total duration | average over active queries per subbucket |
| `join_interval` | `num_joins` | per-subbucket average binary flag |
| `agg_interval` | `num_aggregations` | per-subbucket average binary flag |

### Missing fields and defaults

Redset raw data does not provide direct memory/filter/sort/projection metrics.
To keep `run_pbench.py` compatible, these values are filled as:

| Aggregate column | Value used |
|---|---|
| `avg_memoryused` | empty |
| `proj` | `0.0` |
| `memory_interval` | zero array |
| `filter` | `proxy` (default), or `zero`, or `deterministic` |
| `sort` | `deterministic` (default), or `zero` |
| `filter_interval` | per-subbucket average of filter value |
| `sort_interval` | per-subbucket average of sort value |

### Impact on `run_pbench.py`

- ILP uses: `cputime_sum`, `scanbytes_sum`, `avg_durationtime`, `filter`, `join`, `agg`, `sort`.
- SA uses: `cputime_interval`, `scanbytes_interval`, `filter_interval`, `sort_interval`, `join_interval`, `agg_interval`.
- If filter/sort were always zero, operator matching can collapse and bias synthesis.  
  The script therefore supports deterministic/proxy values to keep these objectives meaningful.

### Run Redset tests

```bash
python -m unittest -q src.Workloads.test_create_redset_aggregates
```
