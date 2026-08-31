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

    def __init__(
        self,
        root_dir: Path | None = None,
        *,
        project: str | None = None,
    ) -> None:
        output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
        configured_dir = (
            output_dir / project / "failures" if project else output_dir / "failures"
        )
        self.project = project
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
        if self.project:
            infos["project"] = self.project
        subdir = self.root_dir / f"r{r}"
        return PrepareResult(payload=data, infos=infos, subdir=subdir)
