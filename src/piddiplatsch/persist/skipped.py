import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.helpers import utc_now
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.result import PrepareResult


class SkipRecorder(RecorderBase):
    LOG_KIND = "skipped"
    LOG_LEVEL = logging.WARNING
    SKIPPED_DIR: Path | None = None

    def __init__(
        self,
        root_dir: Path | None = None,
        *,
        project: str | None = None,
    ) -> None:
        output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
        configured_dir = (
            output_dir / project / "skipped" if project else output_dir / "skipped"
        )
        self.project = project
        super().__init__(
            root_dir or self.SKIPPED_DIR or configured_dir, "skipped_items"
        )

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> PrepareResult:
        timestamp = utc_now().isoformat(timespec="seconds")
        payload = dict(data)
        # Determine retries value: explicit overrides payload value
        payload_retries = retries
        if payload_retries is None:
            payload_retries = int(
                (payload.get("__infos__", {}) or {}).get(
                    "retries", payload.get("retries", 0)
                )
            )
        else:
            # reflect retries in payload for loader semantics
            if "__infos__" in payload:
                payload["__infos__"]["retries"] = payload_retries
            else:
                payload["retries"] = payload_retries

        infos = {
            "skip_timestamp": timestamp,
            "retries": int(payload_retries or 0),
            "reason": reason or "Unknown",
        }
        if self.project:
            infos["project"] = self.project
        return PrepareResult(payload=payload, infos=infos, subdir=None)
