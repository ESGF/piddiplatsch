from __future__ import annotations

from typing import Any

from piddiplatsch.handles.base import HandleBackend
from piddiplatsch.handles.jsonl_backend import JsonlHandleBackend


class RecordingHandleBackend:
    """Write an immutable JSONL audit record before publishing a Handle."""

    def __init__(
        self,
        backend: HandleBackend,
        *,
        project: str | None = None,
        handle_profile: str | None = None,
        output_filename: str | None = None,
        recorder: JsonlHandleBackend | None = None,
    ) -> None:
        self.backend = backend
        self.recorder = recorder or JsonlHandleBackend(
            project=project,
            handle_profile=handle_profile,
            output_filename=output_filename,
        )
        self.prefix = getattr(backend, "prefix", None)

    def add(self, pid: str, record: dict[str, Any]) -> Any:
        # Record first: a direct publication must never bypass the durable audit
        # trail, including when the Handle service subsequently rejects the PUT.
        self.recorder.add(pid, record)
        return self.backend.add(pid, record)

    def get(self, pid: str) -> dict[str, Any] | None:
        return self.backend.get(pid)
