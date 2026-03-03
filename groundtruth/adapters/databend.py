from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from databend_driver import BlockingDatabendClient

from src.utils.databend_exec import build_databend_dsn, execute_databend_query
from src.utils.prometheus import prometheus_queries


class DatabendAdapter:
    def __init__(
        self,
        host: str,
        port: int,
        default_database: str,
        *,
        secure: bool = False,
        prometheus_host: str | None = None,
        prometheus_port: int | None = None,
        prometheus_scrape_wait_s: float = 2.0,
    ):
        self.host = host
        self.port = int(port)
        self.default_database = default_database
        self.secure = secure
        self.prometheus_host = prometheus_host
        self.prometheus_port = int(prometheus_port) if prometheus_port is not None else None
        self.prometheus_scrape_wait_s = float(prometheus_scrape_wait_s)

    @property
    def prometheus_enabled(self) -> bool:
        return bool(self.prometheus_host and self.prometheus_port)

    def _connect(self, database: str, settings: dict[str, Any] | None = None):
        # Keep this aligned with src/utils/databend_exec.py behavior.
        dsn = build_databend_dsn(
            host=self.host,
            port=self.port,
            database=database,
            settings=settings,
            secure=self.secure,
        )
        return BlockingDatabendClient(dsn).get_conn()

    def _query_rows(self, database: str, sql: str, settings: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        conn = self._connect(database, settings=settings)
        try:
            return [dict(r) for r in conn.query_iter(sql)]
        finally:
            conn.close()

    def _explain_plan_text(
        self, database: str, query_text: str, settings: dict[str, Any] | None = None
    ) -> str:
        # Reuse the same explain execution path already used in existing codepaths.
        results = execute_databend_query(
            host=self.host,
            port=self.port,
            database=database,
            query=query_text,
            settings=settings,
            secure=self.secure,
            explain_mode="EXPLAIN ANALYZE",
        )
        # results item shape follows utils.databend_exec.execute_databend_sql:
        # ([], rows, rows). We intentionally only use index 1 to avoid double
        # counting due to legacy duplication of rows at index 2.
        plan_lines: list[str] = []
        for item in results:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            rows = item[1] or []
            for row in rows:
                if isinstance(row, (list, tuple)) and row:
                    plan_lines.append(str(row[0]))
                else:
                    plan_lines.append(str(row))
        return "\n".join(plan_lines)

    @staticmethod
    def _operator_stats_from_plan(plan_text: str) -> dict[str, int]:
        # Count concrete operator node names instead of broad keywords.
        # This is more stable across plan formatting changes.
        def count(pat: str) -> int:
            return len(re.findall(pat, plan_text, flags=re.IGNORECASE))

        joins = count(r"\b(HashJoin|MergeJoin|NestedLoopJoin|CrossJoin|RangeJoin)\b")
        scans = count(r"\b(TableScan|ReadDataSource)\b")
        aggs = count(r"\b(AggregateFinal|AggregatePartial)\b")
        filters = count(r"\bFilter\b|filters:")
        sorts = count(r"\bSort\b")
        return {
            "num_joins": joins,
            "num_scans": scans,
            "num_aggregations": aggs,
            "has_join": 1 if joins > 0 else 0,
            "has_filter": 1 if filters > 0 else 0,
            "has_sort": 1 if sorts > 0 else 0,
            "has_agg": 1 if aggs > 0 else 0,
            "operators_prov": "measured",
        }

    def _prometheus_snapshot(self, ts: float) -> tuple[float, float]:
        if not self.prometheus_enabled:
            return (0.0, 0.0)
        cpu_s = float(prometheus_queries["cpu_new"](self.prometheus_host, self.prometheus_port, ts))
        scan_b = float(prometheus_queries["scan"](self.prometheus_host, self.prometheus_port, ts))
        return (cpu_s, scan_b)

    def check_prometheus_endpoint(self) -> tuple[bool, str]:
        if not self.prometheus_enabled:
            return (True, "disabled")
        url = f"http://{self.prometheus_host}:{self.prometheus_port}/api/v1/status/buildinfo"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code != 200:
                return (False, f"{url} returned HTTP {resp.status_code}")
            payload = resp.json()
            if payload.get("status") != "success":
                return (False, f"{url} returned non-success payload")
            return (True, "ok")
        except Exception as exc:
            return (False, f"{url} unreachable or not Prometheus API ({type(exc).__name__}: {exc})")

    def _execute_sql_with_query_id(
        self, database: str, sql: str, settings: dict[str, Any] | None = None
    ) -> tuple[str | None, str, str | None]:
        """
        Execute SQL and return (engine_query_id, status, error_message).
        Keeps a single connection so we can read `last_query_id()` reliably.
        """
        conn = self._connect(database, settings=settings)
        try:
            try:
                _ = [tuple(r.values()) for r in conn.query_iter(sql)]
                status = "success"
                error = None
            except Exception as query_error:
                try:
                    conn.exec(sql)
                    status = "success"
                    error = None
                except Exception:
                    status = "error"
                    error = str(query_error)
            query_id = None
            try:
                query_id = conn.last_query_id()
            except Exception:
                query_id = None
            return (str(query_id) if query_id else None, status, error)
        finally:
            conn.close()

    def get_engine_version(self) -> str:
        try:
            rows = self._query_rows(self.default_database, "SELECT version() AS v")
            if rows:
                return str(rows[0].get("v") or rows[0].get("version()") or "unknown")
        except Exception:
            pass
        return "unknown"

    def execute(self, *, query_text: str, database: str, client_request_id: str, timeout_s: int) -> dict[str, Any]:
        submit_ts = datetime.now(timezone.utc).isoformat()
        q = f"/* gt:{client_request_id} */ {query_text.strip()}"
        status = "success"
        error = None
        engine_query_id = None
        start_wall = time.time()
        end_wall = None
        # Match llm_gen/databend_exec: use http_handler_result_timeout_secs, not max_execution_time.
        session_settings = {"http_handler_result_timeout_secs": str(timeout_s)}

        operators = None
        try:
            plan_text = self._explain_plan_text(database, query_text, settings=session_settings)
            operators = self._operator_stats_from_plan(plan_text)
        except Exception:
            operators = None

        prom_before = None
        prom_after = None
        if self.prometheus_enabled:
            time.sleep(self.prometheus_scrape_wait_s)
            prom_before = self._prometheus_snapshot(time.time())
        try:
            engine_query_id, status, error = self._execute_sql_with_query_id(
                database, q, settings=session_settings
            )
        finally:
            end_wall = time.time()
            if self.prometheus_enabled:
                time.sleep(self.prometheus_scrape_wait_s)
                prom_after = self._prometheus_snapshot(time.time())

        start_ts = submit_ts
        end_ts = datetime.fromtimestamp(end_wall, tz=timezone.utc).isoformat()
        duration_ms = max(0.0, (end_wall - start_wall) * 1000.0)
        dur_prov = "derived"

        if operators is None:
            has_join = 1 if re.search(r"\bJOIN\b", query_text, flags=re.IGNORECASE) else 0
            has_agg = 1 if re.search(
                r"\bGROUP\s+BY\b|\bCOUNT\s*\(|\bSUM\s*\(|\bAVG\s*\(|\bMIN\s*\(|\bMAX\s*\(",
                query_text,
                flags=re.IGNORECASE,
            ) else 0
            has_sort = 1 if re.search(r"\bORDER\s+BY\b", query_text, flags=re.IGNORECASE) else 0
            has_filter = 1 if re.search(r"\bWHERE\b", query_text, flags=re.IGNORECASE) else 0
            num_joins = float(has_join)
            num_scans = None
            num_aggregations = float(has_agg)
            operators_prov = "proxy"
        else:
            has_join = int(operators["has_join"])
            has_filter = int(operators["has_filter"])
            has_sort = int(operators["has_sort"])
            has_agg = int(operators["has_agg"])
            num_joins = float(operators["num_joins"])
            num_scans = float(operators["num_scans"])
            num_aggregations = float(operators["num_aggregations"])
            operators_prov = operators["operators_prov"]

        prom_cpu_ms = None
        prom_scan_bytes = None
        if prom_before is not None and prom_after is not None:
            prom_cpu_ms = max(0.0, (prom_after[0] - prom_before[0]) * 1000.0)
            prom_scan_bytes = max(0.0, prom_after[1] - prom_before[1])

        scan_bytes = prom_scan_bytes
        cpu_ms = prom_cpu_ms

        return {
            "engine_query_id": engine_query_id,
            "status": status,
            "error_message": error,
            "submit_ts": submit_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_ms": duration_ms,
            "duration_prov": dur_prov,
            "scan_bytes": scan_bytes,
            "cpu_ms": cpu_ms,
            "memory_bytes": None,
            "rows_returned": None,
            "bytes_spilled": None,
            "compile_ms": None,
            "queue_ms": None,
            "execution_ms": duration_ms,
            "num_joins": num_joins,
            "num_scans": num_scans,
            "num_aggregations": num_aggregations,
            "read_table_ids": None,
            "write_table_ids": None,
            "query_type": None,
            "was_aborted": 1 if status != "success" else 0,
            "was_cached": None,
            "has_filter": has_filter,
            "has_sort": has_sort,
            "has_join": has_join,
            "has_agg": has_agg,
            "has_proj": None,
            "scan_bytes_prov": "proxy" if prom_scan_bytes is not None else "missing",
            "cpu_ms_prov": "proxy" if prom_cpu_ms is not None else "missing",
            "memory_bytes_prov": "missing",
            "operators_prov": operators_prov,
        }


def new_client_request_id() -> str:
    return f"gt-{uuid.uuid4()}"
