from __future__ import annotations

import logging
from typing import Any, Literal, Protocol

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend
from piddiplatsch.handles.pyhandle_backend import HandleClient
from piddiplatsch.handles.recording_backend import RecordingHandleBackend
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
        handle_profile: str | None = None,
        output_filename: str | None = None,
    ):
        self.backend: HandleAPIProtocol = backend or get_handle_backend(
            dry_run=dry_run,
            project=project,
            handle_profile=handle_profile,
            output_filename=output_filename,
        )

    def add(self, pid: str, record: dict[str, Any]) -> None:
        self.backend.add(pid, record)

    def get(self, pid: str) -> dict[str, Any] | None:
        return self.backend.get(pid)


# --- Factory Function ---
def get_handle_backend(
    dry_run: bool = False,
    project: str | None = None,
    handle_profile: str | None = None,
    output_filename: str | None = None,
) -> HandleAPIProtocol:
    """
    Return a HandleBackend based on configuration.

    Config keys expected in the project's ``[handles.profiles.*]`` section:
      backend = "rest" | "pyhandle"

    Both publication backends are wrapped by the immutable JSONL audit
    recorder. JSONL-only operation is selected explicitly with dry_run.
    """
    if dry_run:
        logging.warning("Dry-run enabled: using JSONL handle backend")
        return JsonlHandleBackend(
            project=project,
            handle_profile=handle_profile,
            output_filename=output_filename,
        )

    handle_config = config.get_handle(project=project, profile=handle_profile)
    backend_type: Literal["rest", "pyhandle"] = handle_config.get("backend", "rest")
    profile = handle_profile or config.get_handle_profile(project)
    logging.warning(
        "Using Handle backend: %s (project=%s profile=%s)",
        backend_type,
        project or "default",
        profile or "legacy",
    )

    if backend_type == "rest":
        return RecordingHandleBackend(
            RestHandleClient.from_config(project=project, profile=handle_profile),
            project=project,
            handle_profile=handle_profile,
            output_filename=output_filename,
        )

    if backend_type == "pyhandle":
        return RecordingHandleBackend(
            HandleClient.from_config(project=project, profile=handle_profile),
            project=project,
            handle_profile=handle_profile,
            output_filename=output_filename,
        )

    raise ValueError(f"Unknown handle backend type: {backend_type}")
