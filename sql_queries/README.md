# SQL Query Pools

This directory contains the SQL workloads used by the benchmark and metrics
collection scripts.

## Available Query Pools

### `tpch/`

TPC-H query set (template SQL files). These are converted by
`src/Collect_metrics/convert_queries.py` with parameter substitution and a few
dialect compatibility rewrites.

### `tpcds/`

TPC-DS query set. These are mostly used as-is with minimal preprocessing
(mainly comment stripping in the converter script).

### `job/`

JOB / IMDB benchmark queries (Join Order Benchmark). This is the canonical JOB
query pool used by the current metrics converter.

### `ceb/`

CEB query pool, organized by template folder (for example `1a`, `2b`, `11a`).
Queries within a template share the same query structure and differ mainly in
predicate instantiations.

### `redbench/`

Derived workload that combines:

- all JOB queries
- a configurable random sample of CEB queries per template

Layout:

- `redbench/job/*.sql`
- `redbench/ceb/<template>/*.sql`

## Redbench Builder

`populate_redbench.py` builds or refreshes the `redbench/` query pool.

What it does:

1. Copies all files from `sql_queries/job` to `sql_queries/redbench/job`
2. Randomly samples `N` SQL files from each template in `sql_queries/ceb`
3. Copies sampled files into `sql_queries/redbench/ceb/<template>/`

Sampling is deterministic for a given `--seed`.

### Usage

Run from the repository root:

```bash
python3 sql_queries/populate_redbench.py -n 3 --seed 42 --clean
```

This will:

- remove `sql_queries/redbench/job` and `sql_queries/redbench/ceb`
- copy all JOB queries into `sql_queries/redbench/job`
- sample 3 queries per CEB template into `sql_queries/redbench/ceb/<template>/`

### Common Options

- `-n`, `--per-template`: number of CEB queries to sample per template (required)
- `--seed`: random seed for reproducible sampling (default: `42`)
- `--clean`: remove `redbench/job` and `redbench/ceb` before repopulating
- `--clean-ceb`: only remove `redbench/ceb` before re-sampling CEB
- `--skip-job-copy`: only refresh the CEB sample
- `--dry-run`: print actions without modifying files

### Examples

Refresh only the CEB sample (keep existing `redbench/job`):

```bash
python3 sql_queries/populate_redbench.py -n 5 --clean-ceb
```

Preview changes without modifying files:

```bash
python3 sql_queries/populate_redbench.py -n 3 --seed 123 --clean --dry-run
```
