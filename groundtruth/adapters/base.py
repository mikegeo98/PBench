from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAdapter(ABC):
    @abstractmethod
    def execute(self, *, query_text: str, database: str, client_request_id: str, timeout_s: int) -> dict[str, Any]:
        """Execute one statement and return a canonical partial event payload."""

    @abstractmethod
    def get_engine_version(self) -> str:
        """Return engine/version metadata string."""
