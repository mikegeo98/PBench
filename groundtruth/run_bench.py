#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from groundtruth.adapters.databend import DatabendAdapter, new_client_request_id
from groundtruth.io_utils import load_yaml, resolve_output_events_path, write_json, write_jsonl
from groundtruth.schema import GroundTruthEvent


def _load_query_pool(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"Query pool must be a non-empty JSON list: {path}")
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict) or "query" not in row:
            raise ValueError(f"Query pool row {i} missing 'query'")
        query_with_db = str(row["query"])
        if "@" not in query_with_db:
            raise ValueError(f"Query pool row {i} query must include @database suffix")
        query_text, database = query_with_db.rsplit("@", 1)
        out.append(
            {
                "template_id": str(row.get("template_id") or row.get("id") or i),
                "query_text": query_text.strip(),
                "database": database.strip(),
            }
        )
    return out


def _build_plan(config: dict[str, Any], pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
    execution = config.get("execution", {})
    seed = int(execution.get("seed", 42))
    mode = str(execution.get("selection_mode", "as_is"))
    max_queries = int(execution.get("max_queries", len(pool)))

    rng = random.Random(seed)
    selected: list[dict[str, Any]]
    if mode == "shuffle":
        selected = pool[:]
        rng.shuffle(selected)
        selected = selected[:max_queries]
    elif mode == "sample_with_replacement":
        selected = [copy.deepcopy(rng.choice(pool)) for _ in range(max_queries)]
    else:
        selected = pool[:max_queries]

    run_ts = datetime.now(timezone.utc).isoformat()
    plan: list[dict[str, Any]] = []
    for idx, item in enumerate(selected):
        event_id = f"e{idx:07d}"
        plan.append(
            {
                "event_id": event_id,
                "template_id": item["template_id"],
                "query_text": item["query_text"],
                "database": item["database"],
                "scheduled_ts": run_ts,
            }
        )
    return plan


def _make_event(
    *,
    run_id: str,
    benchmark: str,
    engine: str,
    planned: dict[str, Any],
    client_request_id: str,
    result: dict[str, Any],
) -> GroundTruthEvent:
    return GroundTruthEvent(
        run_id=run_id,
        event_id=planned["event_id"],
        benchmark=benchmark,
        engine=engine,
        database=planned["database"],
        template_id=planned.get("template_id"),
        query_text=planned["query_text"],
        client_request_id=client_request_id,
        engine_query_id=result.get("engine_query_id"),
        status=result.get("status", "unknown"),
        error_message=result.get("error_message"),
        submit_ts=result["submit_ts"],
        start_ts=result.get("start_ts"),
        end_ts=result.get("end_ts"),
        duration_ms=result.get("duration_ms"),
        compile_ms=result.get("compile_ms"),
        queue_ms=result.get("queue_ms"),
        execution_ms=result.get("execution_ms"),
        scan_bytes=result.get("scan_bytes"),
        cpu_ms=result.get("cpu_ms"),
        memory_bytes=result.get("memory_bytes"),
        rows_returned=result.get("rows_returned"),
        bytes_spilled=result.get("bytes_spilled"),
        num_joins=result.get("num_joins"),
        num_scans=result.get("num_scans"),
        num_aggregations=result.get("num_aggregations"),
        read_table_ids=result.get("read_table_ids"),
        write_table_ids=result.get("write_table_ids"),
        query_type=result.get("query_type"),
        was_aborted=result.get("was_aborted"),
        was_cached=result.get("was_cached"),
        has_filter=result.get("has_filter"),
        has_sort=result.get("has_sort"),
        has_join=result.get("has_join"),
        has_agg=result.get("has_agg"),
        has_proj=result.get("has_proj"),
        scan_bytes_prov=result.get("scan_bytes_prov", "missing"),
        cpu_ms_prov=result.get("cpu_ms_prov", "missing"),
        memory_bytes_prov=result.get("memory_bytes_prov", "missing"),
        operators_prov=result.get("operators_prov", "missing"),
    )


def _run_one(adapter: DatabendAdapter, planned: dict[str, Any], timeout_s: int) -> tuple[str, str, dict[str, Any]]:
    client_request_id = new_client_request_id()
    try:
        result = adapter.execute(
            query_text=planned["query_text"],
            database=planned["database"],
            client_request_id=client_request_id,
            timeout_s=timeout_s,
        )
    except Exception as exc:
        now = datetime.now(timezone.utc).isoformat()
        result = {
            "engine_query_id": None,
            "status": "error",
            "error_message": str(exc),
            "submit_ts": now,
            "start_ts": now,
            "end_ts": now,
            "duration_ms": 0.0,
            "compile_ms": None,
            "queue_ms": None,
            "execution_ms": 0.0,
            "scan_bytes": None,
            "cpu_ms": None,
            "memory_bytes": None,
            "rows_returned": None,
            "bytes_spilled": None,
            "num_joins": None,
            "num_scans": None,
            "num_aggregations": None,
            "read_table_ids": None,
            "write_table_ids": None,
            "query_type": None,
            "was_aborted": 1,
            "was_cached": None,
            "has_filter": None,
            "has_sort": None,
            "has_join": None,
            "has_agg": None,
            "has_proj": None,
            "scan_bytes_prov": "missing",
            "cpu_ms_prov": "missing",
            "memory_bytes_prov": "missing",
            "operators_prov": "missing",
        }
    return planned["event_id"], client_request_id, result


def run(config: dict[str, Any], output_events_path: str, *, dry_run: bool = False) -> None:
    run_cfg = config.get("run", {})
    engine_cfg = config.get("engine", {})
    workload_cfg = config.get("workload", {})
    exec_cfg = config.get("execution", {})

    run_id = str(run_cfg.get("run_id") or datetime.now(timezone.utc).strftime("run-%Y%m%d-%H%M%S"))
    benchmark = str(workload_cfg.get("benchmark", "unknown"))
    query_pool_path = str(workload_cfg["query_pool_path"])
    timeout_s = int(exec_cfg.get("timeout_s", 300))
    concurrency = int(exec_cfg.get("concurrency", 1))
    mode = str(exec_cfg.get("mode", "sequential"))

    pool = _load_query_pool(query_pool_path)
    plan = _build_plan(config, pool)

    run_dir = Path(output_events_path).parent
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(str(run_dir / "execution_plan.json"), plan)

    if dry_run:
        write_json(
            str(run_dir / "run_manifest.json"),
            {
                "run_id": run_id,
                "benchmark": benchmark,
                "engine": engine_cfg.get("type", "databend"),
                "dry_run": True,
                "planned_events": len(plan),
                "config": config,
            },
        )
        write_jsonl(output_events_path, [])
        return

    if str(engine_cfg.get("type", "databend")).lower() != "databend":
        raise ValueError("Only databend adapter is implemented in this phase")

    adapter = DatabendAdapter(
        host=str(engine_cfg.get("host", "localhost")),
        port=int(engine_cfg.get("port", 8000)),
        default_database=str(engine_cfg.get("default_database", "default")),
    )

    by_event_id: dict[str, GroundTruthEvent] = {}

    if mode == "concurrent" and concurrency > 1:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = [ex.submit(_run_one, adapter, p, timeout_s) for p in plan]
            for fut in as_completed(futures):
                event_id, client_request_id, result = fut.result()
                planned = next(x for x in plan if x["event_id"] == event_id)
                by_event_id[event_id] = _make_event(
                    run_id=run_id,
                    benchmark=benchmark,
                    engine="databend",
                    planned=planned,
                    client_request_id=client_request_id,
                    result=result,
                )
    else:
        for planned in plan:
            event_id, client_request_id, result = _run_one(adapter, planned, timeout_s)
            by_event_id[event_id] = _make_event(
                run_id=run_id,
                benchmark=benchmark,
                engine="databend",
                planned=planned,
                client_request_id=client_request_id,
                result=result,
            )

    ordered_events = [by_event_id[p["event_id"]].to_dict() for p in plan if p["event_id"] in by_event_id]
    write_jsonl(output_events_path, ordered_events)

    write_json(
        str(run_dir / "run_manifest.json"),
        {
            "run_id": run_id,
            "benchmark": benchmark,
            "engine": "databend",
            "engine_version": adapter.get_engine_version(),
            "planned_events": len(plan),
            "written_events": len(ordered_events),
            "config": config,
            "output_events": output_events_path,
        },
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Run benchmark/query-pool and collect groundtruth events")
    p.add_argument("--config", required=True, help="Path to YAML config")
    p.add_argument("--output-events", default=None, help="Output JSONL path")
    p.add_argument("--dry-run", action="store_true", help="Build plan/manifests only")
    args = p.parse_args()

    config = load_yaml(args.config)
    output_events = resolve_output_events_path(config, args.output_events)
    run(config, output_events, dry_run=args.dry_run)
    print(f"Wrote events to {output_events}")


if __name__ == "__main__":
    main()
