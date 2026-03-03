from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

PROVENANCE_VALUES = {"measured", "derived", "proxy", "missing"}


@dataclass
class GroundTruthEvent:
    run_id: str
    event_id: str
    benchmark: str
    engine: str
    database: str
    template_id: str | None
    query_text: str
    client_request_id: str
    engine_query_id: str | None
    status: str
    error_message: str | None

    submit_ts: str
    start_ts: str | None
    end_ts: str | None
    duration_ms: float | None
    compile_ms: float | None
    queue_ms: float | None
    execution_ms: float | None

    scan_bytes: float | None
    cpu_ms: float | None
    memory_bytes: float | None
    rows_returned: float | None
    bytes_spilled: float | None

    num_joins: float | None
    num_scans: float | None
    num_aggregations: float | None
    read_table_ids: str | None
    write_table_ids: str | None
    query_type: str | None
    was_aborted: int | None
    was_cached: int | None

    has_filter: int | None
    has_sort: int | None
    has_join: int | None
    has_agg: int | None
    has_proj: int | None

    scan_bytes_prov: str
    cpu_ms_prov: str
    memory_bytes_prov: str
    operators_prov: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_provenance(value: str) -> str:
    if value not in PROVENANCE_VALUES:
        return "missing"
    return value
