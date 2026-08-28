"""Helpers shared by multiple CLI commands."""

from datetime import datetime
from pathlib import Path

import click

from piddiplatsch.config import config


def select_projects(projects: tuple[str, ...], all_projects: bool) -> str | tuple[str, ...] | None:
    """Translate project options to the selection expected by the pipeline."""
    if projects and all_projects:
        raise click.UsageError("--project cannot be combined with --all-projects")
    return "all" if all_projects else (projects or None)


def resolve_dated_input(
    paths: tuple[Path, ...],
    input_date: datetime | None,
    *,
    relative_path: Path,
    missing_label: str,
) -> tuple[Path, ...]:
    """Resolve explicit paths or one date-based file below the output directory."""
    if paths and input_date is not None:
        raise click.UsageError("PATH cannot be combined with --date")
    if paths:
        return paths
    if input_date is None:
        raise click.UsageError("Provide PATH or --date")

    output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
    path = output_dir / relative_path
    if not path.is_file():
        raise click.ClickException(f"{missing_label} does not exist: {path}")
    return (path,)
