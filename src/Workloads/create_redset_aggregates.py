#!/usr/bin/env python3
"""Create Redset aggregate workload CSVs compatible with PBench.

This script generates Snowset-style aggregate workload files from Redset raw parquet.
It fills missing Redset dimensions with deterministic defaults to keep the
`run_pbench.py` pipeline functional.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


OUTPUT_COLUMNS = [
    "databaseid",
    "qminute",
    "cputime_sum",
    "scanbytes_sum",
    "avg_durationtime",
    "avg_memoryused",
    "join",
    "agg",
    "sort",
    "filter",
    "proj",
    "cputime_interval",
    "scanbytes_interval",
    "duration_interval",
    "memory_interval",
    "filter_interval",
    "sort_interval",
    "agg_interval",
    "join_interval",
]


def _require_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise SystemExit("duckdb is required. Install dependencies and retry.") from exc
    return duckdb


def _parse_ts(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parquet_scan_expr(path: str) -> str:
    p = Path(path)
    if p.is_dir():
        return f"read_parquet('{p.as_posix()}/*.parquet')"
    return f"read_parquet('{p.as_posix()}')"


def _json_zeros(n: int) -> str:
    return json.dumps([0.0] * n)


def resolve_start(
    redset_parquet: str,
    instance_id: str,
    database_id: str,
    start_arg: str | None,
) -> datetime:
    """Resolve start timestamp from CLI or from first arrival in Redset."""
    if start_arg:
        return _parse_ts(start_arg)

    duckdb = _require_duckdb()
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar")
    redset_scan = _parquet_scan_expr(redset_parquet)
    row = con.execute(
        f"""
        SELECT MIN(arrival_timestamp) AS first_arrival
        FROM {redset_scan}
        WHERE CAST(instance_id AS VARCHAR) = ?
          AND CAST(database_id AS VARCHAR) = ?
        """,
        [instance_id, database_id],
    ).fetchone()
    if row is None or row[0] is None:
        raise SystemExit(
            f"No Redset rows found for instance_id={instance_id}, database_id={database_id}"
        )
    dt: datetime = row[0]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class WindowSpec:
    instance_id: str
    database_id: str
    start: datetime
    duration_seconds: int
    slot_seconds: int = 300
    subslot_seconds: int = 30

    @property
    def end(self) -> datetime:
        return self.start + timedelta(seconds=self.duration_seconds)

    @property
    def slot_count(self) -> int:
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if self.duration_seconds % self.slot_seconds != 0:
            raise ValueError("duration_seconds must be divisible by slot_seconds")
        return self.duration_seconds // self.slot_seconds

    @property
    def subslot_count(self) -> int:
        if self.slot_seconds % self.subslot_seconds != 0:
            raise ValueError("slot_seconds must be divisible by subslot_seconds")
        return self.slot_seconds // self.subslot_seconds


def build_rows(
    redset_parquet: str,
    spec: WindowSpec,
    *,
    seed: int = 42,
    filter_mode: str = "proxy",
    sort_mode: str = "deterministic",
) -> list[dict[str, Any]]:
    """Build aggregate rows for one Redset database/window."""
    if filter_mode not in {"zero", "proxy", "deterministic"}:
        raise ValueError("filter_mode must be one of: zero, proxy, deterministic")
    if sort_mode not in {"zero", "deterministic"}:
        raise ValueError("sort_mode must be one of: zero, deterministic")

    duckdb = _require_duckdb()
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar")

    start_utc = spec.start if spec.start.tzinfo else spec.start.replace(tzinfo=timezone.utc)
    start_utc = start_utc.astimezone(timezone.utc)
    end_utc = start_utc + timedelta(seconds=spec.duration_seconds)
    start_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")
    redset_scan = _parquet_scan_expr(redset_parquet)

    # Query-level base set with overlap to the requested window.
    combined_id = f"{spec.instance_id}:{spec.database_id}"
    con.execute(
        f"""
        CREATE TEMP TABLE base_raw AS
        SELECT
          CAST(instance_id AS VARCHAR) AS instance_id,
          CAST(database_id AS VARCHAR) AS database_id,
          CAST(query_id AS BIGINT) AS query_id,
          COALESCE(read_table_ids, '') AS read_table_ids,
          arrival_timestamp AS start_ts,
          arrival_timestamp
            + (
              (COALESCE(compile_duration_ms, 0)
               + COALESCE(queue_duration_ms, 0)
               + COALESCE(execution_duration_ms, 0)) * INTERVAL 1 MILLISECOND
            ) AS finish_ts,
          GREATEST(COALESCE(execution_duration_ms, 0) / 1000.0, 0.0) AS cpu_s_proxy,
          GREATEST(
            (COALESCE(compile_duration_ms, 0)
             + COALESCE(queue_duration_ms, 0)
             + COALESCE(execution_duration_ms, 0)) / 1000.0,
            0.0
          ) AS duration_s,
          GREATEST(COALESCE(mbytes_scanned, 0) / 1024.0, 0.0) AS scan_gb,
          CASE WHEN COALESCE(num_joins, 0) > 0 THEN 1.0 ELSE 0.0 END AS has_join,
          CASE WHEN COALESCE(num_aggregations, 0) > 0 THEN 1.0 ELSE 0.0 END AS has_agg
        FROM {redset_scan}
        WHERE CAST(instance_id AS VARCHAR) = ?
          AND CAST(database_id AS VARCHAR) = ?
          AND COALESCE(was_aborted, 0) = 0
          AND arrival_timestamp < CAST(? AS TIMESTAMP)
          AND (
            arrival_timestamp
              + (
                (COALESCE(compile_duration_ms, 0)
                 + COALESCE(queue_duration_ms, 0)
                 + COALESCE(execution_duration_ms, 0)) * INTERVAL 1 MILLISECOND
              )
          ) > CAST(? AS TIMESTAMP)
        """,
        [spec.instance_id, spec.database_id, end_str, start_str],
    )

    base_count = con.execute("SELECT COUNT(*) FROM base_raw").fetchone()
    if base_count is None or int(base_count[0]) == 0:
        rows: list[dict[str, Any]] = []
        for slot_idx in range(spec.slot_count):
            qminute = start_utc + timedelta(seconds=slot_idx * spec.slot_seconds)
            rows.append(
                {
                    "databaseid": combined_id,
                    "qminute": qminute.strftime("%Y-%m-%d %H:%M:%S"),
                    "cputime_sum": 0.0,
                    "scanbytes_sum": 0.0,
                    "avg_durationtime": None,
                    "avg_memoryused": None,
                    "join": None,
                    "agg": None,
                    "sort": None,
                    "filter": None,
                    "proj": None,
                    "cputime_interval": _json_zeros(spec.subslot_count),
                    "scanbytes_interval": _json_zeros(spec.subslot_count),
                    "duration_interval": _json_zeros(spec.subslot_count),
                    "memory_interval": _json_zeros(spec.subslot_count),
                    "filter_interval": _json_zeros(spec.subslot_count),
                    "sort_interval": _json_zeros(spec.subslot_count),
                    "agg_interval": _json_zeros(spec.subslot_count),
                    "join_interval": _json_zeros(spec.subslot_count),
                }
            )
        return rows

    # Filter proxy baseline by read-table signature (P95 scan per signature).
    con.execute(
        """
        CREATE TEMP TABLE filter_baseline AS
        SELECT
          read_table_ids,
          quantile_cont(scan_gb, 0.95) AS scan_p95_gb
        FROM base_raw
        GROUP BY 1
        """
    )

    filter_expr = "0.0"
    if filter_mode == "deterministic":
        filter_expr = f"CASE WHEN abs(hash(CAST(query_id AS VARCHAR) || ':{seed}:f')) % 100 < 20 THEN 1.0 ELSE 0.0 END"
    elif filter_mode == "proxy":
        filter_expr = (
            "CASE "
            "WHEN fb.scan_p95_gb IS NULL OR fb.scan_p95_gb <= 0 THEN 0.0 "
            "ELSE LEAST(1.0, GREATEST(0.0, 1.0 - (br.scan_gb / fb.scan_p95_gb))) "
            "END"
        )

    sort_expr = "0.0"
    if sort_mode == "deterministic":
        sort_expr = f"CASE WHEN abs(hash(CAST(query_id AS VARCHAR) || ':{seed}:s')) % 100 < 10 THEN 1.0 ELSE 0.0 END"

    con.execute(
        f"""
        CREATE TEMP TABLE base AS
        SELECT
          br.*,
          GREATEST(epoch(br.finish_ts) - epoch(br.start_ts), 1e-6) AS runtime_s,
          {filter_expr} AS filter_value,
          {sort_expr} AS sort_value
        FROM base_raw br
        LEFT JOIN filter_baseline fb USING (read_table_ids)
        """
    )

    con.execute(
        """
        CREATE TEMP TABLE slots AS
        SELECT
          slot_idx,
          CAST(? AS TIMESTAMP) + (slot_idx * ? * INTERVAL 1 SECOND) AS slot_start,
          CAST(? AS TIMESTAMP) + ((slot_idx + 1) * ? * INTERVAL 1 SECOND) AS slot_end
        FROM generate_series(0, ?) AS t(slot_idx)
        """,
        [start_str, spec.slot_seconds, start_str, spec.slot_seconds, spec.slot_count - 1],
    )

    con.execute(
        """
        CREATE TEMP TABLE slot_overlap AS
        SELECT
          s.slot_idx,
          b.query_id,
          GREATEST(
            0.0,
            epoch(LEAST(b.finish_ts, s.slot_end)) - epoch(GREATEST(b.start_ts, s.slot_start))
          ) AS overlap_s,
          b.runtime_s,
          b.cpu_s_proxy,
          b.scan_gb,
          b.duration_s,
          b.has_join,
          b.has_agg,
          b.filter_value,
          b.sort_value
        FROM slots s
        JOIN base b
          ON b.start_ts < s.slot_end
         AND b.finish_ts > s.slot_start
        """
    )

    slot_rows = con.execute(
        """
        SELECT
          slot_idx,
          SUM(cpu_s_proxy * overlap_s / runtime_s) AS cputime_sum,
          SUM(scan_gb * overlap_s / runtime_s) AS scanbytes_sum,
          AVG(duration_s) AS avg_durationtime,
          AVG(has_join) AS join_ratio,
          AVG(has_agg) AS agg_ratio,
          AVG(sort_value) AS sort_ratio,
          AVG(filter_value) AS filter_ratio
        FROM slot_overlap
        GROUP BY 1
        """
    ).fetchall()

    con.execute(
        """
        CREATE TEMP TABLE subslots AS
        SELECT
          slot_idx,
          bin_idx,
          CAST(? AS TIMESTAMP)
            + (slot_idx * ? * INTERVAL 1 SECOND)
            + (bin_idx * ? * INTERVAL 1 SECOND) AS bin_start,
          CAST(? AS TIMESTAMP)
            + (slot_idx * ? * INTERVAL 1 SECOND)
            + ((bin_idx + 1) * ? * INTERVAL 1 SECOND) AS bin_end
        FROM generate_series(0, ?) AS s(slot_idx)
        CROSS JOIN generate_series(0, ?) AS b(bin_idx)
        """,
        [
            start_str,
            spec.slot_seconds,
            spec.subslot_seconds,
            start_str,
            spec.slot_seconds,
            spec.subslot_seconds,
            spec.slot_count - 1,
            spec.subslot_count - 1,
        ],
    )

    con.execute(
        """
        CREATE TEMP TABLE subslot_overlap AS
        SELECT
          ss.slot_idx,
          ss.bin_idx,
          b.query_id,
          GREATEST(
            0.0,
            epoch(LEAST(b.finish_ts, ss.bin_end)) - epoch(GREATEST(b.start_ts, ss.bin_start))
          ) AS overlap_s,
          b.runtime_s,
          b.cpu_s_proxy,
          b.scan_gb,
          b.duration_s,
          b.has_join,
          b.has_agg,
          b.filter_value,
          b.sort_value
        FROM subslots ss
        JOIN base b
          ON b.start_ts < ss.bin_end
         AND b.finish_ts > ss.bin_start
        """
    )

    bin_rows = con.execute(
        """
        SELECT
          slot_idx,
          bin_idx,
          SUM(cpu_s_proxy * overlap_s / runtime_s) AS cpu_bin,
          SUM(scan_gb * overlap_s / runtime_s) AS scan_bin,
          AVG(duration_s) AS duration_bin,
          AVG(filter_value) AS filter_bin,
          AVG(sort_value) AS sort_bin,
          AVG(has_agg) AS agg_bin,
          AVG(has_join) AS join_bin
        FROM subslot_overlap
        GROUP BY 1,2
        """
    ).fetchall()

    rows_by_slot: dict[int, dict[str, Any]] = {}
    for slot_idx in range(spec.slot_count):
        qminute = start_utc + timedelta(seconds=slot_idx * spec.slot_seconds)
        rows_by_slot[slot_idx] = {
            "databaseid": combined_id,
            "qminute": qminute.strftime("%Y-%m-%d %H:%M:%S"),
            "cputime_sum": 0.0,
            "scanbytes_sum": 0.0,
            "avg_durationtime": None,
            "avg_memoryused": None,
            "join": None,
            "agg": None,
            "sort": None,
            "filter": None,
            "proj": None,
            "cputime_interval": [0.0] * spec.subslot_count,
            "scanbytes_interval": [0.0] * spec.subslot_count,
            "duration_interval": [0.0] * spec.subslot_count,
            "memory_interval": [0.0] * spec.subslot_count,
            "filter_interval": [0.0] * spec.subslot_count,
            "sort_interval": [0.0] * spec.subslot_count,
            "agg_interval": [0.0] * spec.subslot_count,
            "join_interval": [0.0] * spec.subslot_count,
        }

    for (
        slot_idx,
        cputime_sum,
        scanbytes_sum,
        avg_durationtime,
        join_ratio,
        agg_ratio,
        sort_ratio,
        filter_ratio,
    ) in slot_rows:
        row = rows_by_slot[int(slot_idx)]
        row["cputime_sum"] = float(cputime_sum)
        row["scanbytes_sum"] = float(scanbytes_sum)
        row["avg_durationtime"] = float(avg_durationtime)
        row["join"] = float(join_ratio)
        row["agg"] = float(agg_ratio)
        row["sort"] = float(sort_ratio)
        row["filter"] = float(filter_ratio)
        row["proj"] = 0.0

    for (
        slot_idx,
        bin_idx,
        cpu_bin,
        scan_bin,
        duration_bin,
        filter_bin,
        sort_bin,
        agg_bin,
        join_bin,
    ) in bin_rows:
        row = rows_by_slot[int(slot_idx)]
        idx = int(bin_idx)
        row["cputime_interval"][idx] = float(cpu_bin)
        row["scanbytes_interval"][idx] = float(scan_bin)
        row["duration_interval"][idx] = float(duration_bin)
        row["filter_interval"][idx] = float(filter_bin)
        row["sort_interval"][idx] = float(sort_bin)
        row["agg_interval"][idx] = float(agg_bin)
        row["join_interval"][idx] = float(join_bin)

    out: list[dict[str, Any]] = []
    for slot_idx in range(spec.slot_count):
        row = rows_by_slot[slot_idx]
        for k in [
            "cputime_interval",
            "scanbytes_interval",
            "duration_interval",
            "memory_interval",
            "filter_interval",
            "sort_interval",
            "agg_interval",
            "join_interval",
        ]:
            row[k] = json.dumps(row[k])
        out.append(row)
    return out


def write_rows(rows: list[dict[str, Any]], output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create Redset aggregate workload CSV.")
    p.add_argument("--redset-parquet", required=True, help="Path to Redset parquet file/dir")
    p.add_argument("--instance-id", required=True, help="Redset instance_id (cluster id)")
    p.add_argument("--database-id", required=True, help="Redset database_id")
    p.add_argument(
        "--start",
        required=False,
        default=None,
        help="Window start timestamp (ISO-8601). If omitted, use first timestamp for the selected instance/database pair.",
    )
    p.add_argument("--duration-seconds", type=int, required=True, help="Window duration in seconds")
    p.add_argument("--bucket-seconds", type=int, default=300, help="Slot width in seconds")
    p.add_argument("--subbucket-seconds", type=int, default=30, help="Subslot width in seconds")
    p.add_argument("--seed", type=int, default=42, help="Seed for deterministic defaults")
    p.add_argument(
        "--filter-mode",
        choices=["zero", "proxy", "deterministic"],
        default="proxy",
        help="How to derive filter targets",
    )
    p.add_argument(
        "--sort-mode",
        choices=["zero", "deterministic"],
        default="deterministic",
        help="How to derive sort targets",
    )
    p.add_argument("--output", required=True, help="Output CSV path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spec = WindowSpec(
        instance_id=args.instance_id,
        database_id=args.database_id,
        start=resolve_start(args.redset_parquet, args.instance_id, args.database_id, args.start),
        duration_seconds=args.duration_seconds,
        slot_seconds=args.bucket_seconds,
        subslot_seconds=args.subbucket_seconds,
    )
    rows = build_rows(
        args.redset_parquet,
        spec,
        seed=args.seed,
        filter_mode=args.filter_mode,
        sort_mode=args.sort_mode,
    )
    write_rows(rows, args.output)


if __name__ == "__main__":
    main()
