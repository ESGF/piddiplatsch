from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from piddiplatsch.utils.models import build_handle, prepare_handle_data


class HandleBackend(ABC):
    """
    Abstract backend template. Implements create-or-update logic.
    """

    def add(self, pid: str, record: dict[str, Any]) -> Any:
        handle, handle_data = self._prepare(pid, record)
        return self._store(handle, handle_data)

    def get(self, pid: str) -> dict[str, Any] | None:
        """Retrieve a stored handle record for the given PID.

        Returns a dict of fields (including 'URL') or None if not found.
        """
        handle = build_handle(pid)
        return self._retrieve(handle)

    def _prepare(self, pid: str, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        handle = build_handle(pid)
        handle_data = prepare_handle_data(record)

        if "URL" not in handle_data or not handle_data["URL"]:
            raise ValueError("Missing required 'URL' in record")

        return handle, handle_data

    @abstractmethod
    def _store(self, handle: str, handle_data: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        raise NotImplementedError
