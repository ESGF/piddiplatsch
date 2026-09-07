"""Helpers shared by multiple CLI commands."""

import re
from datetime import date, datetime, time, timedelta
from pathlib import Path

import click

from piddiplatsch.config import config

MAP_DATE_FORMATS = "YYYY-MM-DD, today, yesterday, today-N, or last"


def parse_map_date(value: str, *, today: date | None = None) -> datetime | str:
    """Parse an absolute or relative map date selector."""
    normalized = value.strip().lower()
    if normalized == "last":
        return normalized

    current_date = today or date.today()
    if normalized == "today":
        selected_date = current_date
    elif normalized == "yesterday":
        selected_date = current_date - timedelta(days=1)
    elif match := re.fullmatch(r"today-(\d+)", normalized):
        selected_date = current_date - timedelta(days=int(match.group(1)))
    else:
        try:
            selected_date = date.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"must be one of: {MAP_DATE_FORMATS}") from exc
        if selected_date.isoformat() != normalized:
            raise ValueError(f"must be one of: {MAP_DATE_FORMATS}")

    return datetime.combine(selected_date, time.min)


def select_projects(
    projects: tuple[str, ...], all_projects: bool
) -> str | tuple[str, ...] | None:
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
