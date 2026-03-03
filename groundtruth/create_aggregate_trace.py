#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from groundtruth.io_utils import read_jsonl
from src.Workloads.create_redset_aggregates import WindowSpec as RedWindowSpec, build_rows as build_red_rows, write_rows as write_red_rows
from src.Workloads.create_snowset_aggregates import WindowSpec as SnowWindowSpec, build_rows as build_snow_rows, write_rows as write_snow_rows


def _infer_start_end(events: list[dict]) -> tuple[datetime, datetime]:
    ts = []
    for e in events:
        v = e.get("start_ts") or e.get("submit_ts")
        if v:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts.append(dt.astimezone(timezone.utc))
    if not ts:
        raise ValueError("No timestamps found in events")
    start = min(ts)
    end = max(ts)
    return start, end


def _duration_s(start: datetime, end: datetime, min_seconds: int = 300) -> int:
    d = int((end - start).total_seconds())
    return max(min_seconds, d + 1)


def _prepare_snowset_parquets(events_path: str, out_dir: str, database_id: str) -> tuple[str, str]:
    con = duckdb.connect()
    events_scan = f"read_json_auto('{events_path}')"
    main_path = str(Path(out_dir) / "snowset-main.parquet")
    ts_path = str(Path(out_dir) / "ts-explosion.parquet")

    con.execute(
        f"""
        COPY (
            SELECT
              CAST(row_number() OVER () AS BIGINT) AS index,
              CAST(coalesce(try_cast(engine_query_id AS BIGINT), row_number() OVER ()) AS BIGINT) AS queryId,
              CAST('{database_id}' AS VARCHAR) AS databaseId,
              CAST(coalesce(start_ts, submit_ts) AS TIMESTAMP) AS createdTime,
              CAST(coalesce(end_ts, start_ts, submit_ts) AS TIMESTAMP) AS endTime,
              CAST(coalesce(try_cast(duration_ms AS DOUBLE), try_cast(execution_ms AS DOUBLE), 0) AS BIGINT) AS durationTotal,
              CAST(coalesce(try_cast(cpu_ms AS DOUBLE), 0) * 1000 AS BIGINT) AS userCpuTime,
              CAST(0 AS BIGINT) AS systemCpuTime,
              CAST(coalesce(try_cast(scan_bytes AS DOUBLE), 0) AS BIGINT) AS scanBytes,
              CAST(coalesce(try_cast(memory_bytes AS DOUBLE), 0) AS BIGINT) AS memoryUsed,
              CAST(CASE WHEN coalesce(has_filter,0) > 0 THEN 1 ELSE 0 END AS BIGINT) AS profFilterRso,
              CAST(CASE WHEN coalesce(has_sort,0) > 0 THEN 1 ELSE 0 END AS BIGINT) AS profSortRso,
              CAST(CASE WHEN coalesce(has_agg,0) > 0 THEN 1 ELSE 0 END AS BIGINT) AS profAggRso,
              CAST(CASE WHEN coalesce(has_join,0) > 0 THEN 1 ELSE 0 END AS BIGINT) AS profHjRso,
              CAST(CASE WHEN coalesce(has_proj,0) > 0 THEN 1 ELSE 0 END AS BIGINT) AS profProjRso
            FROM {events_scan}
            WHERE coalesce(status, 'success') = 'success'
        ) TO '{main_path}' (FORMAT PARQUET)
        """
    )

    con.execute(
        f"""
        COPY (
            WITH base AS (
              SELECT
                CAST(coalesce(try_cast(engine_query_id AS BIGINT), row_number() OVER ()) AS BIGINT) AS queryId,
                date_trunc('second', CAST(coalesce(start_ts, submit_ts) AS TIMESTAMP)) AS start_sec,
                date_trunc('second', CAST(coalesce(end_ts, start_ts, submit_ts) AS TIMESTAMP)) AS end_sec
              FROM {events_scan}
              WHERE coalesce(status, 'success') = 'success'
            )
            SELECT
              CAST(row_number() OVER () AS BIGINT) AS index,
              sec AS sec,
              queryId
            FROM base, generate_series(start_sec, end_sec, INTERVAL 1 SECOND) t(sec)
        ) TO '{ts_path}' (FORMAT PARQUET)
        """
    )

    return main_path, ts_path


def _prepare_redset_parquet(events_path: str, out_dir: str, instance_id: str, database_id: str) -> str:
    con = duckdb.connect()
    events_scan = f"read_json_auto('{events_path}')"
    out_path = str(Path(out_dir) / "redset.parquet")
    instance_id_i = int(instance_id)
    database_id_i = int(database_id)
    con.execute(
        f"""
        COPY (
            SELECT
              CAST({instance_id_i} AS INTEGER) AS instance_id,
              CAST({database_id_i} AS BIGINT) AS database_id,
              CAST(coalesce(try_cast(engine_query_id AS BIGINT), row_number() OVER ()) AS BIGINT) AS query_id,
              CAST(coalesce(start_ts, submit_ts) AS TIMESTAMP) AS arrival_timestamp,
              CAST(coalesce(try_cast(compile_ms AS DOUBLE), 0) AS DOUBLE) AS compile_duration_ms,
              CAST(coalesce(try_cast(queue_ms AS DOUBLE), 0) AS BIGINT) AS queue_duration_ms,
              CAST(coalesce(try_cast(execution_ms AS DOUBLE), try_cast(duration_ms AS DOUBLE), 0) AS BIGINT) AS execution_duration_ms,
              CAST(coalesce(try_cast(scan_bytes AS DOUBLE), 0) / 1048576.0 AS DOUBLE) AS mbytes_scanned,
              CAST(coalesce(try_cast(bytes_spilled AS DOUBLE), 0) / 1048576.0 AS DOUBLE) AS mbytes_spilled,
              CAST(coalesce(try_cast(num_joins AS DOUBLE), try_cast(has_join AS DOUBLE), 0) AS BIGINT) AS num_joins,
              CAST(coalesce(try_cast(num_scans AS DOUBLE), 0) AS BIGINT) AS num_scans,
              CAST(coalesce(try_cast(num_aggregations AS DOUBLE), try_cast(has_agg AS DOUBLE), 0) AS BIGINT) AS num_aggregations,
              CAST(coalesce(try_cast(read_table_ids AS VARCHAR), '') AS VARCHAR) AS read_table_ids,
              CAST(coalesce(try_cast(write_table_ids AS VARCHAR), '') AS VARCHAR) AS write_table_ids,
              CAST(coalesce(try_cast(query_type AS VARCHAR), '') AS VARCHAR) AS query_type,
              CAST(coalesce(try_cast(was_aborted AS INTEGER), CASE WHEN coalesce(status,'success')='success' THEN 0 ELSE 1 END) AS INTEGER) AS was_aborted,
              CAST(coalesce(try_cast(was_cached AS INTEGER), 0) AS INTEGER) AS was_cached,
              CAST(NULL AS INTEGER) AS cache_source_query_id,
              CAST(NULL AS BIGINT) AS user_id,
              CAST(NULL AS DOUBLE) AS cluster_size,
              CAST(NULL AS VARCHAR) AS feature_fingerprint,
              CAST(NULL AS DOUBLE) AS num_permanent_tables_accessed,
              CAST(NULL AS DOUBLE) AS num_external_tables_accessed,
              CAST(NULL AS DOUBLE) AS num_system_tables_accessed
            FROM {events_scan}
        ) TO '{out_path}' (FORMAT PARQUET)
        """
    )
    return out_path


def main() -> None:
    p = argparse.ArgumentParser(description="Create redset/snowset aggregate workload CSV from groundtruth events")
    p.add_argument("--input-events", required=True, help="Path to events JSONL from run_bench")
    p.add_argument("--format", required=True, choices=["snowset", "redset"])
    p.add_argument("--output", required=True, help="Output aggregate CSV")
    p.add_argument("--database-id", default="0", help="Database id to stamp (numeric-like string recommended)")
    p.add_argument("--instance-id", default="0", help="Redset instance id")
    p.add_argument("--duration-seconds", type=int, default=None)
    p.add_argument("--bucket-seconds", type=int, default=300)
    p.add_argument("--subbucket-seconds", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--filter-mode", choices=["zero", "proxy", "deterministic"], default="proxy")
    p.add_argument("--sort-mode", choices=["zero", "deterministic"], default="deterministic")
    args = p.parse_args()

    events = read_jsonl(args.input_events)
    if not events:
        raise SystemExit("No events in input file")

    start, end = _infer_start_end(events)
    duration_seconds = args.duration_seconds or _duration_s(start, end)

    with tempfile.TemporaryDirectory(prefix="gt-agg-") as tmp:
        if args.format == "snowset":
            main_pq, ts_pq = _prepare_snowset_parquets(args.input_events, tmp, args.database_id)
            spec = SnowWindowSpec(
                database_id=args.database_id,
                start=start,
                duration_seconds=duration_seconds,
                slot_seconds=args.bucket_seconds,
                subslot_seconds=args.subbucket_seconds,
            )
            rows = build_snow_rows(main_pq, ts_pq, spec)
            write_snow_rows(rows, args.output)
        else:
            red_pq = _prepare_redset_parquet(args.input_events, tmp, args.instance_id, args.database_id)
            spec = RedWindowSpec(
                instance_id=args.instance_id,
                database_id=args.database_id,
                start=start,
                duration_seconds=duration_seconds,
                slot_seconds=args.bucket_seconds,
                subslot_seconds=args.subbucket_seconds,
            )
            rows = build_red_rows(
                red_pq,
                spec,
                seed=args.seed,
                filter_mode=args.filter_mode,
                sort_mode=args.sort_mode,
            )
            write_red_rows(rows, args.output)

    print(f"Wrote {args.format} aggregate CSV to {args.output}")


if __name__ == "__main__":
    main()
