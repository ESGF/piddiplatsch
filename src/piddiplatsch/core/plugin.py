from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .processing import BaseProcessor


class ProcessorFactory(Protocol):
    def __call__(self, **kwargs) -> BaseProcessor:  # pragma: no cover
        ...


def dataset_version_index(item_id: str) -> tuple[str, int]:
    """Default index for built-in projects' final ``.v<digits>`` component."""
    dataset_id, separator, version = item_id.rpartition(".")
    if not separator or not dataset_id or not re.fullmatch(r"v[0-9]+", version):
        raise ValueError(f"Invalid versioned dataset identifier: {item_id!r}")
    return dataset_id, int(version[1:])


@dataclass(frozen=True)
class PluginSpec:
    """Metadata and factory for one project processing plugin."""

    name: str
    project_ids: tuple[str, ...]
    make_processor: ProcessorFactory
    description: str | None = None
    catalog_index: Callable[[str], tuple[str, int]] = dataset_version_index

    def __post_init__(self) -> None:
        name = normalize_project_id(self.name)
        project_ids = tuple(
            dict.fromkeys(normalize_project_id(value) for value in self.project_ids)
        )
        if not name:
            raise ValueError("Plugin name must not be empty")
        if not project_ids or any(not value for value in project_ids):
            raise ValueError(f"Plugin '{name}' must declare at least one project id")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "project_ids", project_ids)

    def matches(self, project_id: str) -> bool:
        return normalize_project_id(project_id) in self.project_ids


def normalize_project_id(value: str) -> str:
    """Normalize plugin names and publication project identifiers."""
    if not isinstance(value, str):
        return ""
    return value.strip().casefold()
