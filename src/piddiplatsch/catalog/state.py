"""Durable replay identity and restartable project cleaning for harvest."""

import json
import os
import re
import tempfile
from pathlib import Path
from uuid import uuid4

from piddiplatsch.exceptions import CatalogError


class HarvestState:
    """Small control file independent of the project database backend.

    A pending clean is saved with a fresh Kafka group generation before any
    catalog is reset. Startup finishes pending resets before consuming. Once
    pending is cleared, the generation remains stable across normal restarts so
    Kafka can resume a partially completed rebuild. This assumes one harvest
    command operates on the catalog directory at a time.
    """

    def __init__(self, directory: Path):
        self.path = directory / ".harvest-state.json"
        self.generation: str | None = None
        self.pending_clean: tuple[str, ...] = ()
        try:
            if not self.path.exists():
                return
            data = json.loads(self.path.read_text(encoding="utf-8"))
            generation = data["generation"]
            pending = data["pending_clean"]
            if data["version"] != 1:
                raise ValueError("unsupported harvest state version")
            if not isinstance(generation, str) or not re.fullmatch(
                r"[0-9a-f]{32}", generation
            ):
                raise ValueError("invalid harvest generation")
            if not isinstance(pending, list) or any(
                not isinstance(name, str)
                or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name)
                for name in pending
            ):
                raise ValueError("invalid pending project names")
            self.generation = generation
            self.pending_clean = tuple(pending)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CatalogError(
                f"Could not read harvest state {self.path}: {exc}"
            ) from exc

    @property
    def kafka_group_suffix(self) -> str:
        return ".harvest" + (f".{self.generation}" if self.generation else "")

    def request_clean(self, projects: tuple[str, ...]) -> None:
        """Persist a new replay generation and all projects awaiting reset."""
        if any(not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", name) for name in projects):
            raise CatalogError("Invalid project name for catalog cleaning")
        pending = tuple(dict.fromkeys((*self.pending_clean, *projects)))
        self._save(uuid4().hex, pending)

    def complete_clean(self) -> None:
        """Mark resets complete before the first Kafka message is consumed."""
        if self.pending_clean:
            self._save(self.generation, ())

    def _save(self, generation: str, pending: tuple[str, ...]) -> None:
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=".harvest-state-",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(
                    {"version": 1, "generation": generation, "pending_clean": pending},
                    stream,
                )
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.path)
            # Persist the rename as well as the contents on POSIX filesystems.
            if os.name == "posix":
                descriptor = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            self.generation = generation
            self.pending_clean = pending
        except OSError as exc:
            raise CatalogError(
                f"Could not persist harvest state {self.path}: {exc}"
            ) from exc
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
