# Redbench Workload Builder

This folder contains a helper script to populate a `redbench` workload from:

- all JOB queries (`sql_queries/job`)
- a random sample of CEB queries per template (`sql_queries/ceb/<template>`)

The script writes the result into this folder using a consistent layout:

- JOB queries: `sql_queries/redbench/job/*.sql`
- CEB queries: `sql_queries/redbench/ceb/*.sql`

CEB files are stored in a flat folder and prefixed with the template name to avoid filename collisions, for example:

- `1a__1a1336.sql`
- `11a__9945f4c9f5c35268de6b932bfe3e1020145f5fe7.sql`

## Script

- `populate_redbench.py`

What it does:

1. Copies all files from `sql_queries/job` into `sql_queries/redbench/job`
2. Randomly samples `N` SQL files from each CEB template folder in `sql_queries/ceb`
3. Copies sampled CEB files into `sql_queries/redbench/ceb`

Sampling is deterministic if you pass the same `--seed`.

## Usage

Run from the repository root:

```bash
python3 sql_queries/redbench/populate_redbench.py -n 3 --seed 42 --clean
```

This will:

- remove `sql_queries/redbench/job` and `sql_queries/redbench/ceb`
- copy all JOB queries into `sql_queries/redbench/job`
- sample 3 queries per CEB template into `sql_queries/redbench/ceb`

## Common options

- `-n`, `--per-template`: number of CEB queries to sample per template (required)
- `--seed`: random seed for reproducible sampling (default: `42`)
- `--clean`: remove `redbench/job` and `redbench/ceb` before repopulating (keeps this script)
- `--clean-ceb`: only remove `redbench/ceb` before re-sampling CEB
- `--skip-job-copy`: only refresh the CEB sample
- `--dry-run`: print actions without modifying files

## Examples

Sample 5 queries per CEB template, keep existing JOB files:

```bash
python3 sql_queries/redbench/populate_redbench.py -n 5 --clean-ceb
```

Preview what would happen without changing files:

```bash
python3 sql_queries/redbench/populate_redbench.py -n 3 --seed 123 --clean --dry-run
```
