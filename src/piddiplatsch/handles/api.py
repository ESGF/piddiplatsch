from __future__ import annotations

import logging
from typing import Any, Literal, Protocol

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.handles.pyhandle_backend import HandleClient
from piddiplatsch.handles.rest_backend import RestHandleClient


class HandleAPIProtocol(Protocol):
    """Protocol defining the public Handle API for processors."""

    def add(self, pid: str, record: dict[str, Any]) -> None: ...
    def get(self, pid: str) -> dict[str, Any] | None: ...


class HandleAPI(HandleAPIProtocol):
    """User-facing API wrapping a backend."""

    def __init__(
        self,
        backend: HandleBackend | None = None,
        *,
        dry_run: bool = False,
        project: str | None = None,
    ):
        self.backend: HandleBackend = backend or get_handle_backend(
            dry_run=dry_run,
            project=project,
        )

    def add(self, pid: str, record: dict[str, Any]) -> None:
        self.backend.add(pid, record)

    def get(self, pid: str) -> dict[str, Any] | None:
        return self.backend.get(pid)


# --- Factory Function ---
def get_handle_backend(
    dry_run: bool = False,
    project: str | None = None,
) -> HandleBackend:
    """
    Return a HandleBackend based on configuration.

    Config keys expected in [handle] section:
      backend = "rest" | "pyhandle" | "jsonl"
      jsonl_path = "test-handles.jsonl"  # only for jsonl
    """
    if dry_run:
        logging.warning("Dry-run enabled: using JSONL handle backend")
        return JsonlHandleBackend(project=project)

    backend_type: Literal["rest", "pyhandle", "jsonl"] = config.get(
        "handle", "backend", fallback="rest"
    )
    logging.warning(f"Using handle backend: {backend_type}")

    if backend_type == "rest":
        return RestHandleClient.from_config()

    if backend_type == "pyhandle":
        return HandleClient.from_config()

    if backend_type == "jsonl":
        return JsonlHandleBackend(project=project)

    raise ValueError(f"Unknown handle backend type: {backend_type}")
