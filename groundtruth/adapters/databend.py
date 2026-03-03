from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from databend_driver import BlockingDatabendClient

from src.utils.databend_exec import build_databend_dsn


class DatabendAdapter:
    def __init__(self, host: str, port: int, default_database: str, *, secure: bool = False):
        self.host = host
        self.port = int(port)
        self.default_database = default_database
        self.secure = secure
        self._query_log_shape: dict[str, str] | None = None

    def _connect(self, database: str, settings: dict[str, Any] | None = None):
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

    def execute(self, *, query_text: str, database: str, client_request_id: str, timeout_s: int) -> dict[str, Any]:
        submit_ts = datetime.now(timezone.utc).isoformat()
        q = f"/* gt:{client_request_id} */ {query_text.strip()}"
        status = "success"
        error = None
        start_wall = time.time()
        end_wall = None
        conn = self._connect(database, settings={"max_execution_time": timeout_s})
        try:
            _ = [tuple(r.values()) for r in conn.query_iter(q)]
        except Exception as exc:
            status = "error"
            error = str(exc)
        finally:
            conn.close()
            end_wall = time.time()

        log_row = self._lookup_query_log(database, client_request_id)

        start_ts = self._safe_iso(log_row.get("start_ts")) if log_row else submit_ts
        end_ts = self._safe_iso(log_row.get("end_ts")) if log_row else datetime.fromtimestamp(end_wall, tz=timezone.utc).isoformat()

        if log_row and log_row.get("duration_ms") is not None:
            duration_ms = float(log_row["duration_ms"])
            dur_prov = "measured"
        else:
            duration_ms = max(0.0, (end_wall - start_wall) * 1000.0)
            dur_prov = "derived"

        query_upper = query_text.upper()
        has_join = 1 if " JOIN " in f" {query_upper} " else 0
        has_agg = 1 if any(x in query_upper for x in [" GROUP BY ", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("]) else 0
        has_sort = 1 if " ORDER BY " in query_upper else 0
        has_filter = 1 if " WHERE " in query_upper else 0

        return {
            "engine_query_id": str(log_row.get("query_id")) if log_row and log_row.get("query_id") is not None else None,
            "status": status,
            "error_message": error,
            "submit_ts": submit_ts,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_ms": duration_ms,
            "duration_prov": dur_prov,
            "scan_bytes": float(log_row.get("scan_bytes")) if log_row and log_row.get("scan_bytes") is not None else None,
            "cpu_ms": float(log_row.get("cpu_ms")) if log_row and log_row.get("cpu_ms") is not None else None,
            "memory_bytes": float(log_row.get("memory_bytes")) if log_row and log_row.get("memory_bytes") is not None else None,
            "rows_returned": float(log_row.get("result_rows")) if log_row and log_row.get("result_rows") is not None else None,
            "bytes_spilled": float(log_row.get("spilled_bytes")) if log_row and log_row.get("spilled_bytes") is not None else None,
            "compile_ms": None,
            "queue_ms": None,
            "execution_ms": duration_ms,
            "num_joins": float(has_join),
            "num_scans": None,
            "num_aggregations": float(has_agg),
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
            "scan_bytes_prov": "measured" if log_row and log_row.get("scan_bytes") is not None else "missing",
            "cpu_ms_prov": "measured" if log_row and log_row.get("cpu_ms") is not None else "missing",
            "memory_bytes_prov": "measured" if log_row and log_row.get("memory_bytes") is not None else "missing",
            "operators_prov": "proxy",
        }


def new_client_request_id() -> str:
    return f"gt-{uuid.uuid4()}"
