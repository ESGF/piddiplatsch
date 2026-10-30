from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

from piddiplatsch.config import config
from piddiplatsch.handles.base import HandleBackend
from piddiplatsch.helpers import DailyJsonlWriter, utc_now


class JsonlHandleBackend(HandleBackend):
    """
    JSONL backend for testing, similar to DumpRecorder.
    Writes one file per day under the configured dump directory.
    Format per line:
      {"handle": "<HANDLE>", "URL": "<location>", "data": {...},
       "timestamp": "<ISO8601>", "project": "<plugin>"}
    """

    def __init__(
        self,
        project: str | None = None,
        handle_profile: str | None = None,
        output_filename: str | None = None,
    ):
        # Read output_dir from [consumer] section, fallback to outputs
        base_dir = config.get("consumer", {}).get("output_dir", "outputs")
        self.project = project
        self.output_filename = output_filename
        self._output_date: date | None = None
        self.prefix = config.get_handle(project=project, profile=handle_profile).get(
            "prefix", ""
        )
        dump_dir = Path(base_dir)
        if project:
            dump_dir /= project
        dump_dir /= "handles"
        # Use shared JSONL writer for daily rotation and append semantics
        self.writer = DailyJsonlWriter(Path(dump_dir))

    def set_output_date(self, day: date | None) -> None:
        """Route catalog mapping to its source event day; named batches take precedence."""
        self._output_date = day

    def _store(self, handle: str, handle_data: dict[str, Any]) -> None:
        # Include timestamp for traceability; let writer choose the dated filename
        record = {
            "handle": handle,
            "URL": handle_data.pop("URL", None),
            "data": handle_data,
            "timestamp": utc_now().isoformat(),
        }
        if self.project:
            record["project"] = self.project
        filename = self.output_filename or (
            f"handles_{self._output_date.isoformat()}.jsonl"
            if self._output_date
            else None
        )
        path = self.writer.write("handles", record, filename=filename)
        logging.debug(f"Wrote handle {handle} to {path}")

    def _retrieve(self, handle: str) -> dict[str, Any] | None:
        """Retrieval is intentionally not supported for JSONL backend.

        Returning None avoids slow file scans during tests and runtime.
        """
        logging.debug("JSONL handle backend does not support retrieval; returning None")
        return None
