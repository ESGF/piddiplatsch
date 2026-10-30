"""Configured paths and backend selection for project catalogs."""

from pathlib import Path

from piddiplatsch.catalog.base import BaseCatalog
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin
from piddiplatsch.exceptions import CatalogError


def get_catalog_directory() -> Path:
    return Path(
        config.get("catalog", "directory")
        or Path(config.get("consumer", "output_dir", "outputs")) / "catalog"
    )


def get_catalog_path(
    project: str,
    *,
    directory: Path | None = None,
    backend: str | None = None,
) -> Path:
    project = get_plugin(project).name
    if Path(project).name != project or project in {".", ".."}:
        raise CatalogError(f"Unsafe catalog project name: {project!r}")
    backend = backend or config.get("catalog", "backend", "sqlite")
    if backend != "sqlite":
        raise CatalogError(f"Unsupported catalog backend: {backend!r}")
    return (directory or get_catalog_directory()) / f"{project}.sqlite"


def open_catalog(
    project: str,
    *,
    directory: Path | None = None,
    backend: str | None = None,
    read_only: bool = False,
) -> BaseCatalog:
    path = get_catalog_path(project, directory=directory, backend=backend)
    project = get_plugin(project).name
    from piddiplatsch.catalog.sqlite import SQLiteCatalog

    return SQLiteCatalog(
        project,
        path,
        read_only=read_only,
    )
