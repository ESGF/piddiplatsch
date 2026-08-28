import logging
import time
from typing import Any, Protocol

from piddiplatsch.handles.api import HandleAPI, HandleAPIProtocol
from piddiplatsch.result import ProcessingResult


class BaseProcessor:
    def __init__(
        self,
        handle_backend: HandleAPIProtocol | None = None,
        retries: int = 0,
        dry_run: bool = False,
        project: str | None = None,
        handle_profile: str | None = None,
    ):
        self.handle_backend: HandleAPIProtocol = handle_backend or HandleAPI(
            dry_run=dry_run,
            project=project,
            handle_profile=handle_profile,
        )
        self.handle_profile = handle_profile
        self.retries = retries
        self.logger = logging.getLogger(__name__)

    def __str__(self):
        """Return a friendly name for progress display."""
        return self.__class__.__name__.replace("Processor", "").lower()

    def _safe_add_record(self, record) -> None:
        last_err = None
        for attempt in range(1, self.retries + 2):
            try:
                self.handle_backend.add(record.pid, record.as_record())
                return
            except Exception as e:
                last_err = e
                self.logger.warning(
                    f"Attempt {attempt} failed to add PID {record.pid}: {e}"
                )
        raise RuntimeError(
            f"Failed to register handle {record.pid} after {self.retries + 1} attempts"
        ) from last_err

    def _time_function(self, func, *args, **kwargs) -> tuple[Any, float]:
        t0 = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        return result, elapsed

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        raise NotImplementedError


class ProcessingPlugin(Protocol):
    """Protocol for processing plugins.

    Optional preflight check and required message processing.
    """

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None: ...

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult: ...
