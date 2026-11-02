"""Exclusive harvest ownership and the manual work-rebuild interlock."""

import fcntl
from contextlib import contextmanager
from pathlib import Path

from piddiplatsch.exceptions import CatalogError


@contextmanager
def harvest_lock(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".harvest.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def ensure_no_work_rebuild(directory):
    if (Path(directory) / ".work-rebuild.json").exists():
        raise CatalogError(
            "A work rebuild is incomplete; stop processing and run "
            "piddi admin rebuild --apply to recover it before continuing"
        )
