"""Helpers for replaying individual JSON fixture files."""

import json
from pathlib import Path

from piddiplatsch.core.pipeline import process_messages


def feed_test_files(
    testfile_paths,
    projects: list[str] | tuple[str, ...] | str = ("cmip6",),
):
    messages = []
    for path in testfile_paths:
        if isinstance(path, str):
            path = Path(path)
        with path.open("r", encoding="utf-8") as f:
            messages.append((path.name, json.load(f)))
    process_messages(messages, projects=projects)
