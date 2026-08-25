import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.helpers import utc_now
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.result import PrepareResult


class FailureRecorder(RecorderBase):
    LOG_KIND = "failure"
    LOG_LEVEL = logging.WARNING
    FAILURE_DIR: Path | None = None

    def __init__(self, root_dir: Path | None = None) -> None:
        configured_dir = (
            Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
        )
        super().__init__(root_dir or self.FAILURE_DIR or configured_dir, "failed_items")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> PrepareResult:
        ts = utc_now().isoformat(timespec="seconds")
        r = 0 if retries is None else int(retries)
        infos = {"failure_timestamp": ts, "retries": r, "reason": reason or "Unknown"}
        subdir = self.root_dir / f"r{r}"
        return PrepareResult(payload=data, infos=infos, subdir=subdir)
