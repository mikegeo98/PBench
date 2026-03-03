from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from databend_driver import BlockingDatabendClient

from src.utils.databend_exec import build_databend_dsn
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
        self._query_log_shape: dict[str, str] | None = None
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

    def _execute_sql(self, database: str, sql: str, settings: dict[str, Any] | None = None) -> None:
        """Execute SQL similarly to src/utils/databend_exec.execute_databend_sql."""
        conn = self._connect(database, settings=settings)
        try:
            try:
                _ = [tuple(r.values()) for r in conn.query_iter(sql)]
            except Exception as query_error:
                try:
                    conn.exec(sql)
                except Exception:
                    raise query_error
        finally:
            conn.close()

    def _explain_plan_text(
        self, database: str, query_text: str, settings: dict[str, Any] | None = None
    ) -> str:
        rows = self._query_rows(database, f"EXPLAIN {query_text.strip()}", settings=settings)
        out: list[str] = []
        for row in rows:
            for v in row.values():
                if v is not None:
                    out.append(str(v))
        return "\n".join(out)

    @staticmethod
    def _operator_stats_from_plan(plan_text: str) -> dict[str, int]:
        def count(pat: str) -> int:
            return len(re.findall(pat, plan_text, flags=re.IGNORECASE))

        joins = count(r"\b(HashJoin|MergeJoin|NestedLoopJoin|CrossJoin|Join)\b")
        scans = count(r"\b(TableScan|Scan|ReadDataSource)\b")
        aggs = count(r"\b(AggregateFinal|AggregatePartial|Aggregate|GroupBy)\b")
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

    def _discover_query_log(self, database: str) -> dict[str, str] | None:
        if self._query_log_shape is not None:
            return self._query_log_shape

        candidates = ["query_log", "query_history"]
        table = None
        try:
            tbl_rows = self._query_rows(database, "SHOW TABLES FROM system")
            names = {str(next(iter(r.values()))).lower() for r in tbl_rows if r}
            for c in candidates:
                if c in names:
                    table = c
                    break
        except Exception:
            table = None

        if not table:
            self._query_log_shape = None
            return None

        try:
            desc_rows = self._query_rows(database, f"DESCRIBE system.{table}")
        except Exception:
            self._query_log_shape = None
            return None

        cols = {str(r.get("Field") or r.get("field") or "").lower(): str(r.get("Field") or r.get("field") or "") for r in desc_rows}

        def pick(*options: str) -> str | None:
            for o in options:
                if o.lower() in cols:
                    return cols[o.lower()]
            return None

        shape = {
            "table": table,
            "query_id": pick("query_id", "id") or "query_id",
            "query_text": pick("query", "query_text", "sql") or "query",
            "start": pick("query_start_time", "start_time", "event_time", "created_time") or "query_start_time",
            "end": pick("query_finish_time", "finish_time", "end_time") or "query_finish_time",
            "duration_ms": pick("query_duration_ms", "duration_ms") or "query_duration_ms",
            "scan_bytes": pick("scan_bytes", "read_bytes") or "scan_bytes",
            "result_rows": pick("result_rows", "written_rows", "rows") or "result_rows",
            "cpu_ms": pick("cpu_time_ms", "cpu_ms") or "cpu_time_ms",
            "memory_bytes": pick("memory_usage", "memory_bytes", "memory_used") or "memory_usage",
            "spilled_bytes": pick("spilled_bytes", "spill_bytes") or "spilled_bytes",
            "state": pick("state", "status") or "state",
        }
        self._query_log_shape = shape
        return shape

    @staticmethod
    def _safe_iso(v: Any) -> str | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc).isoformat()
        s = str(v)
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return s

    def _lookup_query_log(self, database: str, client_request_id: str) -> dict[str, Any] | None:
        shape = self._discover_query_log(database)
        if not shape:
            return None

        token_like = client_request_id.replace("'", "''")
        sql = (
            "SELECT "
            f"{shape['query_id']} AS query_id, "
            f"{shape['query_text']} AS query_text, "
            f"{shape['start']} AS start_ts, "
            f"{shape['end']} AS end_ts, "
            f"{shape['duration_ms']} AS duration_ms, "
            f"{shape['scan_bytes']} AS scan_bytes, "
            f"{shape['result_rows']} AS result_rows, "
            f"{shape['cpu_ms']} AS cpu_ms, "
            f"{shape['memory_bytes']} AS memory_bytes, "
            f"{shape['spilled_bytes']} AS spilled_bytes, "
            f"{shape['state']} AS state "
            f"FROM system.{shape['table']} "
            f"WHERE {shape['query_text']} LIKE '%{token_like}%' "
            "ORDER BY start_ts DESC LIMIT 1"
        )
        try:
            rows = self._query_rows(database, sql)
            return rows[0] if rows else None
        except Exception:
            return None

    def _lookup_query_log_by_query_id(self, database: str, query_id: str) -> dict[str, Any] | None:
        shape = self._discover_query_log(database)
        if not shape:
            return None
        qid = query_id.replace("'", "''")
        sql = (
            "SELECT "
            f"{shape['query_id']} AS query_id, "
            f"{shape['query_text']} AS query_text, "
            f"{shape['start']} AS start_ts, "
            f"{shape['end']} AS end_ts, "
            f"{shape['duration_ms']} AS duration_ms, "
            f"{shape['scan_bytes']} AS scan_bytes, "
            f"{shape['result_rows']} AS result_rows, "
            f"{shape['cpu_ms']} AS cpu_ms, "
            f"{shape['memory_bytes']} AS memory_bytes, "
            f"{shape['spilled_bytes']} AS spilled_bytes, "
            f"{shape['state']} AS state "
            f"FROM system.{shape['table']} "
            f"WHERE {shape['query_id']} = '{qid}' "
            "ORDER BY start_ts DESC LIMIT 1"
        )
        try:
            rows = self._query_rows(database, sql)
            return rows[0] if rows else None
        except Exception:
            return None

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

        log_row = None
        for _ in range(3):
            if engine_query_id:
                log_row = self._lookup_query_log_by_query_id(database, engine_query_id)
            if log_row is None:
                log_row = self._lookup_query_log(database, client_request_id)
            if log_row is not None:
                break
            time.sleep(0.2)

        start_ts = self._safe_iso(log_row.get("start_ts")) if log_row else submit_ts
        end_ts = self._safe_iso(log_row.get("end_ts")) if log_row else datetime.fromtimestamp(end_wall, tz=timezone.utc).isoformat()

        if log_row and log_row.get("duration_ms") is not None:
            duration_ms = float(log_row["duration_ms"])
            dur_prov = "measured"
        else:
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

        log_scan_bytes = float(log_row.get("scan_bytes")) if log_row and log_row.get("scan_bytes") is not None else None
        log_cpu_ms = float(log_row.get("cpu_ms")) if log_row and log_row.get("cpu_ms") is not None else None
        scan_bytes = log_scan_bytes if log_scan_bytes is not None else prom_scan_bytes
        cpu_ms = log_cpu_ms if log_cpu_ms is not None else prom_cpu_ms

        return {
            "engine_query_id": str(log_row.get("query_id")) if log_row and log_row.get("query_id") is not None else engine_query_id,
            "status": status,
            "error_message": error,
            "submit_ts": submit_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_ms": duration_ms,
            "duration_prov": dur_prov,
            "scan_bytes": scan_bytes,
            "cpu_ms": cpu_ms,
            "memory_bytes": float(log_row.get("memory_bytes")) if log_row and log_row.get("memory_bytes") is not None else None,
            "rows_returned": float(log_row.get("result_rows")) if log_row and log_row.get("result_rows") is not None else None,
            "bytes_spilled": float(log_row.get("spilled_bytes")) if log_row and log_row.get("spilled_bytes") is not None else None,
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
            "scan_bytes_prov": (
                "measured" if log_scan_bytes is not None else ("proxy" if prom_scan_bytes is not None else "missing")
            ),
            "cpu_ms_prov": (
                "measured" if log_cpu_ms is not None else ("proxy" if prom_cpu_ms is not None else "missing")
            ),
            "memory_bytes_prov": "measured" if log_row and log_row.get("memory_bytes") is not None else "missing",
            "operators_prov": operators_prov,
        }


def new_client_request_id() -> str:
    return f"gt-{uuid.uuid4()}"
