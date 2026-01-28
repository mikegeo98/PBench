#!/usr/bin/env python3
"""Create Snowset aggregate workload CSVs from raw parquet inputs.

This script rebuilds a single aggregate workload window for one Snowset
database id. It is designed to match the existing aggregate format in:
`src/Workloads/Snowset/workload1h-5m-30s_*.csv`.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _require_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise SystemExit(
            "duckdb is required. Install dependencies via uv/pip and retry."
        ) from exc
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


@dataclass(frozen=True)
class WindowSpec:
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
        if self.duration_seconds % self.slot_seconds != 0:
            raise ValueError("duration_seconds must be divisible by slot_seconds")
        return self.duration_seconds // self.slot_seconds

    @property
    def subslot_count(self) -> int:
        if self.slot_seconds % self.subslot_seconds != 0:
            raise ValueError("slot_seconds must be divisible by subslot_seconds")
        return self.slot_seconds // self.subslot_seconds


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


def build_rows(
    snowset_main_parquet: str,
    ts_explosion_parquet: str,
    spec: WindowSpec,
) -> list[dict[str, Any]]:
    duckdb = _require_duckdb()
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar")

    start_utc = spec.start if spec.start.tzinfo is not None else spec.start.replace(tzinfo=timezone.utc)
    start_utc = start_utc.astimezone(timezone.utc)
    end_utc = start_utc + timedelta(seconds=spec.duration_seconds)
    start_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")
    main_scan = _parquet_scan_expr(snowset_main_parquet)
    ts_scan = _parquet_scan_expr(ts_explosion_parquet)

    con.execute(
        f"""
        CREATE TEMP TABLE base AS
        SELECT
          queryId,
          CAST(databaseId AS VARCHAR) AS databaseid,
          createdTime,
          endTime,
          date_diff('second', date_trunc('second', createdTime), date_trunc('second', endTime)) + 1 AS n_total,
          (userCpuTime + systemCpuTime) / 1000000.0 AS cpu_s,
          scanBytes / POWER(1024, 3) AS scan_gb,
          durationTotal / 1000.0 AS duration_total_s,
          memoryUsed / POWER(1024, 3) AS memory_gb,
          CASE WHEN profFilterRso > 0 THEN 1 ELSE 0 END AS has_filter,
          CASE WHEN profSortRso > 0 THEN 1 ELSE 0 END AS has_sort,
          CASE WHEN profAggRso > 0 THEN 1 ELSE 0 END AS has_agg,
          CASE WHEN profHjRso > 0 THEN 1 ELSE 0 END AS has_join,
          CASE WHEN profProjRso > 0 THEN 1 ELSE 0 END AS has_proj
        FROM {main_scan}
        WHERE CAST(databaseId AS VARCHAR) = ?
          AND createdTime < CAST(? AS TIMESTAMP)
          AND endTime >= CAST(? AS TIMESTAMP)
        """,
        [spec.database_id, end_str, start_str],
    )

    base_count = con.execute("SELECT COUNT(*) FROM base").fetchone()
    if base_count is None or int(base_count[0]) == 0:
        # No overlapping queries in this window; emit empty slots.
        rows: list[dict[str, Any]] = []
        for slot_idx in range(spec.slot_count):
            qminute = start_utc + timedelta(seconds=slot_idx * spec.slot_seconds)
            row = {
                "databaseid": spec.database_id,
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
                "cputime_interval": json.dumps([0.0] * spec.subslot_count),
                "scanbytes_interval": json.dumps([0.0] * spec.subslot_count),
                "duration_interval": json.dumps([0.0] * spec.subslot_count),
                "memory_interval": json.dumps([0.0] * spec.subslot_count),
                "filter_interval": json.dumps([0.0] * spec.subslot_count),
                "sort_interval": json.dumps([0.0] * spec.subslot_count),
                "agg_interval": json.dumps([0.0] * spec.subslot_count),
                "join_interval": json.dumps([0.0] * spec.subslot_count),
            }
            rows.append(row)
        return rows

    con.execute(
        f"""
        CREATE TEMP TABLE ts_window AS
        SELECT
          t.queryId,
          CAST(FLOOR((epoch(t.sec) - epoch(CAST(? AS TIMESTAMP))) / ?) AS BIGINT) AS slot_idx,
          CAST(FLOOR(((epoch(t.sec) - epoch(CAST(? AS TIMESTAMP))) % ?) / ?) AS BIGINT) AS bin_idx,
          COUNT(*) AS n_bin
        FROM {ts_scan} t
        JOIN base b USING (queryId)
        WHERE t.sec >= CAST(? AS TIMESTAMP)
          AND t.sec < CAST(? AS TIMESTAMP)
        GROUP BY 1,2,3
        HAVING slot_idx >= 0 AND slot_idx < ?
        """,
        [
            start_str,
            spec.slot_seconds,
            start_str,
            spec.slot_seconds,
            spec.subslot_seconds,
            start_str,
            end_str,
            spec.slot_count,
        ],
    )

    # Query-level activity per slot.
    con.execute(
        """
        CREATE TEMP TABLE query_slot AS
        SELECT queryId, slot_idx, SUM(n_bin) AS n_slot
        FROM ts_window
        GROUP BY 1,2
        """
    )

    slot_rows = con.execute(
        """
        SELECT
          qs.slot_idx,
          SUM(b.cpu_s * qs.n_slot::DOUBLE / b.n_total) AS cputime_sum,
          SUM(b.scan_gb * qs.n_slot::DOUBLE / b.n_total) AS scanbytes_sum,
          AVG(b.duration_total_s) AS avg_durationtime,
          AVG(b.memory_gb) AS avg_memoryused,
          AVG(b.has_join) AS join_ratio,
          AVG(b.has_agg) AS agg_ratio,
          AVG(b.has_sort) AS sort_ratio,
          AVG(b.has_filter) AS filter_ratio,
          AVG(b.has_proj) AS proj_ratio
        FROM query_slot qs
        JOIN base b USING (queryId)
        GROUP BY 1
        """
    ).fetchall()

    bin_rows = con.execute(
        """
        SELECT
          tw.slot_idx,
          tw.bin_idx,
          SUM(b.cpu_s * tw.n_bin::DOUBLE / b.n_total) AS cpu_bin,
          SUM(b.scan_gb * tw.n_bin::DOUBLE / b.n_total) AS scan_bin,
          AVG(b.duration_total_s) AS duration_bin,
          AVG(b.memory_gb) AS memory_bin,
          AVG(b.has_filter) AS filter_bin,
          AVG(b.has_sort) AS sort_bin,
          AVG(b.has_agg) AS agg_bin,
          AVG(b.has_join) AS join_bin
        FROM ts_window tw
        JOIN base b USING (queryId)
        GROUP BY 1,2
        """
    ).fetchall()

    row_by_slot: dict[int, dict[str, Any]] = {}
    for slot_idx in range(spec.slot_count):
        qminute = start_utc + timedelta(seconds=slot_idx * spec.slot_seconds)
        row_by_slot[slot_idx] = {
            "databaseid": spec.database_id,
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
        avg_memoryused,
        join_ratio,
        agg_ratio,
        sort_ratio,
        filter_ratio,
        proj_ratio,
    ) in slot_rows:
        slot = row_by_slot[int(slot_idx)]
        slot["cputime_sum"] = float(cputime_sum)
        slot["scanbytes_sum"] = float(scanbytes_sum)
        slot["avg_durationtime"] = float(avg_durationtime)
        slot["avg_memoryused"] = float(avg_memoryused)
        slot["join"] = float(join_ratio)
        slot["agg"] = float(agg_ratio)
        slot["sort"] = float(sort_ratio)
        slot["filter"] = float(filter_ratio)
        slot["proj"] = float(proj_ratio)

    for (
        slot_idx,
        bin_idx,
        cpu_bin,
        scan_bin,
        duration_bin,
        memory_bin,
        filter_bin,
        sort_bin,
        agg_bin,
        join_bin,
    ) in bin_rows:
        slot = row_by_slot[int(slot_idx)]
        idx = int(bin_idx)
        slot["cputime_interval"][idx] = float(cpu_bin)
        slot["scanbytes_interval"][idx] = float(scan_bin)
        slot["duration_interval"][idx] = float(duration_bin)
        slot["memory_interval"][idx] = float(memory_bin)
        slot["filter_interval"][idx] = float(filter_bin)
        slot["sort_interval"][idx] = float(sort_bin)
        slot["agg_interval"][idx] = float(agg_bin)
        slot["join_interval"][idx] = float(join_bin)

    rows: list[dict[str, Any]] = []
    for slot_idx in range(spec.slot_count):
        row = row_by_slot[slot_idx]
        for key in [
            "cputime_interval",
            "scanbytes_interval",
            "duration_interval",
            "memory_interval",
            "filter_interval",
            "sort_interval",
            "agg_interval",
            "join_interval",
        ]:
            row[key] = json.dumps(row[key])
        rows.append(row)
    return rows


def write_rows(rows: list[dict[str, Any]], output_path: str) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create Snowset aggregate workload CSV from parquet inputs."
    )
    p.add_argument("--snowset-main-parquet", required=True, help="Parquet file/dir path")
    p.add_argument("--ts-explosion-parquet", required=True, help="Parquet file/dir path")
    p.add_argument("--database-id", required=True, help="Snowset databaseId")
    p.add_argument(
        "--start",
        required=True,
        help="Window start timestamp (ISO-8601, e.g. 2018-02-22T08:35:00Z)",
    )
    p.add_argument(
        "--duration-seconds",
        type=int,
        required=True,
        help="Window duration in seconds (e.g. 3600)",
    )
    p.add_argument(
        "--bucket-seconds",
        type=int,
        default=300,
        help="Slot width in seconds (default: 300)",
    )
    p.add_argument(
        "--subbucket-seconds",
        type=int,
        default=30,
        help="Sub-interval width in seconds (default: 30)",
    )
    p.add_argument("--output", required=True, help="Output CSV file path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spec = WindowSpec(
        database_id=args.database_id,
        start=_parse_ts(args.start),
        duration_seconds=args.duration_seconds,
        slot_seconds=args.bucket_seconds,
        subslot_seconds=args.subbucket_seconds,
    )
    rows = build_rows(args.snowset_main_parquet, args.ts_explosion_parquet, spec)
    write_rows(rows, args.output)


if __name__ == "__main__":
    main()
