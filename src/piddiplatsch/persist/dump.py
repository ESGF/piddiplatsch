import logging
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.persist.base import RecorderBase
from piddiplatsch.result import PrepareResult


class DumpRecorder(RecorderBase):
    LOG_KIND = "dump"
    LOG_LEVEL = logging.DEBUG
    DUMP_DIR: Path | None = None

    def __init__(self, root_dir: Path | None = None) -> None:
        configured_dir = (
            Path(config.get("consumer", {}).get("output_dir", "outputs")) / "dump"
        )
        super().__init__(root_dir or self.DUMP_DIR or configured_dir, "dump_messages")

    def prepare(
        self,
        key: str,
        data: dict,
        reason: str | None,
        retries: int | None,
    ) -> PrepareResult:
        # No infos, just write raw payload
        return PrepareResult(payload=data)
