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
