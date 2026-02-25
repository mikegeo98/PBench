#!/usr/bin/env python3
"""Convert PBench SA plans to flat query trace CSV.

Output schema:
    arrival_timestamp,query_type,sql,read_tables,write_table

The converter expands `sa_plan/*-plan2.json` files (5-minute slots, 30-second
subslots by default) into one row per query execution. Multiple plans can be
concatenated sequentially into one longer trace.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List


READ_JOIN_RE = re.compile(r"\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)", re.IGNORECASE)
FROM_CLAUSE_RE = re.compile(
    r"\bFROM\b(.*?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)
TABLE_TOKEN_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\b")
UPDATE_RE = re.compile(r"\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
INSERT_RE = re.compile(r"\bINSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
DELETE_RE = re.compile(r"\bDELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
TRUNCATE_RE = re.compile(r"\bTRUNCATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
MERGE_RE = re.compile(r"\bMERGE\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def _parse_ts(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_ts(dt: datetime) -> str:
    # Match the user-requested style (space separator, microseconds).
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _split_sql_and_db(query: str) -> tuple[str, str | None]:
    if "@" in query:
        sql, db = query.rsplit("@", 1)
        return sql.strip(), db.strip()
    return query.strip(), None


def _split_statements(sql: str) -> list[str]:
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def _query_type(sql: str) -> str:
    statements = _split_statements(sql)
    if not statements:
        return "unknown"
    kinds: list[str] = []
    for stmt in statements:
        head = stmt.lstrip().split(None, 1)[0].lower() if stmt.strip() else ""
        kinds.append(head)
    for kind in ("insert", "update", "delete", "merge", "truncate"):
        if kind in kinds:
            return kind
    return kinds[0] if kinds else "unknown"


def _extract_read_tables(sql: str) -> list[str]:
    tables: list[str] = []
    seen: set[str] = set()
    for stmt in _split_statements(sql):
        # Parse comma-join style FROM lists.
        m_from = FROM_CLAUSE_RE.search(stmt)
        if m_from:
            from_part = m_from.group(1)
            for seg in from_part.split(","):
                seg = seg.strip()
                if not seg:
                    continue
                # Ignore JOIN pieces here; handled below.
                if re.search(r"\bJOIN\b", seg, re.IGNORECASE):
                    continue
                tok = TABLE_TOKEN_RE.match(seg)
                if tok:
                    t = tok.group(1).split(".")[-1]
                    if t not in seen:
                        seen.add(t)
                        tables.append(t)
        # Parse explicit JOIN chains.
        for m in READ_JOIN_RE.finditer(stmt):
            t = m.group(1).split(".")[-1]
            if t not in seen:
                seen.add(t)
                tables.append(t)
    return tables


def _extract_write_table(sql: str) -> str:
    for stmt in _split_statements(sql):
        for rx in (INSERT_RE, UPDATE_RE, DELETE_RE, TRUNCATE_RE, MERGE_RE):
            m = rx.search(stmt)
            if m:
                return m.group(1)
    return ""


@dataclass(frozen=True)
class TraceRow:
    arrival_timestamp: str
    query_type: str
    sql: str
    read_tables: str
    write_table: str


def _expand_plan_rows(
    sa_plan: list[dict],
    start: datetime,
    *,
    slot_seconds: int,
    subslot_seconds: int,
    spread_within_subslot: bool,
) -> list[TraceRow]:
    rows: list[TraceRow] = []
    for slot_idx, slot in enumerate(sa_plan):
        bins = slot.get("queries", []) or []
        for sub_idx, queries in enumerate(bins):
            if not queries:
                continue
            subslot_start = start + timedelta(seconds=slot_idx * slot_seconds + sub_idx * subslot_seconds)
            n = len(queries)
            for q_idx, raw_query in enumerate(queries):
                sql, _db = _split_sql_and_db(raw_query)
                sql_out = _normalize_sql(sql)
                if spread_within_subslot and n > 1:
                    # Deterministic spread inside the 30s bucket.
                    offset_us = int((q_idx * subslot_seconds * 1_000_000) / n)
                    ts = subslot_start + timedelta(microseconds=offset_us)
                else:
                    ts = subslot_start
                rows.append(
                    TraceRow(
                        arrival_timestamp=_format_ts(ts),
                        query_type=_query_type(sql_out),
                        sql=sql_out if sql_out.endswith(";") else sql_out + ";",
                        read_tables=",".join(_extract_read_tables(sql_out)),
                        write_table=_extract_write_table(sql_out),
                    )
                )
    return rows


def _load_sa_plan(path: Path) -> list[dict]:
    with path.open("r") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} does not contain a SA plan list")
    return data


def _slot_count(sa_plan: list[dict]) -> int:
    return len(sa_plan)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert PBench SA plan(s) to trace CSV.")
    p.add_argument(
        "--sa-plan",
        dest="sa_plans",
        nargs="+",
        required=True,
        help="One or more SA plan JSON files (e.g. output/sa_plan/.../redbench-plan2.json).",
    )
    p.add_argument(
        "--start",
        required=True,
        help="Start timestamp for the first plan (ISO8601, e.g. 2024-03-01T07:51:14Z).",
    )
    p.add_argument("--output", required=True, help="Output CSV path.")
    p.add_argument("--slot-seconds", type=int, default=300, help="Slot duration in seconds (default: 300).")
    p.add_argument("--subslot-seconds", type=int, default=30, help="Subslot duration in seconds (default: 30).")
    p.add_argument(
        "--no-spread-within-subslot",
        action="store_true",
        help="Use exact subslot start for all queries in a subslot instead of deterministic spreading.",
    )
    p.add_argument(
        "--preserve-input-order",
        action="store_true",
        help="Do not sort input SA plan paths. Default behavior sorts paths lexicographically before concatenation.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    sa_paths = [Path(p) for p in args.sa_plans]
    if not args.preserve_input_order:
        sa_paths = sorted(sa_paths)

    current_start = _parse_ts(args.start)
    all_rows: list[TraceRow] = []

    for path in sa_paths:
        sa_plan = _load_sa_plan(path)
        all_rows.extend(
            _expand_plan_rows(
                sa_plan,
                current_start,
                slot_seconds=args.slot_seconds,
                subslot_seconds=args.subslot_seconds,
                spread_within_subslot=not args.no_spread_within_subslot,
            )
        )
        current_start = current_start + timedelta(seconds=_slot_count(sa_plan) * args.slot_seconds)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["arrival_timestamp", "query_type", "sql", "read_tables", "write_table"])
        for r in all_rows:
            writer.writerow([r.arrival_timestamp, r.query_type, r.sql, r.read_tables, r.write_table])

    print(f"Wrote {len(all_rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
