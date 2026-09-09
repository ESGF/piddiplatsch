"""Complete Click interface for piddiplatsch."""

from datetime import datetime
from pathlib import Path

import click

from piddiplatsch.commands import (
    ConfigShowCommand,
    ConfigValidateCommand,
    ConsumeCommand,
    HarvestCommand,
    MapCommand,
    PublishCommand,
    RetryCommand,
    TopCommand,
)
from piddiplatsch.commands.helper import DATE_FORMATS, parse_date_selector
from piddiplatsch.config import config

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
DEFAULT_USER_CONFIG = "custom.toml"


def _parse_date_selector(
    _ctx: click.Context, _param: click.Parameter, value: str | None
) -> datetime | str | None:
    if value is None:
        return None
    try:
        return parse_date_selector(value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option(
    "-c",
    "--config",
    "config_file",
    type=click.Path(),
    default=DEFAULT_USER_CONFIG,
    show_default=True,
    help=(
        "Local config TOML loaded after /etc/piddi/piddi.toml "
        "(replaces the default ./custom.toml layer)."
    ),
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option(
    "-v/-s",
    "--verbose/--silent",
    default=True,
    help="Show progress information (enabled by default).",
)
@click.option(
    "-l",
    "--log",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default="pid.log",
    show_default=True,
    help="Log file path.",
)
@click.pass_context
def cli(
    ctx: click.Context, config_file: str | None, debug: bool, verbose: bool, log: str
) -> None:
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    config.load_config_layers(config_file)
    config.configure_logging(debug=debug, log=log)


# command consume


@cli.command()
@click.option(
    "--publish",
    is_flag=True,
    help="Also publish mapped Handles immediately; JSONL is always written first.",
)
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Project plugin to run; repeat to select several (overrides config).",
)
@click.option(
    "--all-projects",
    is_flag=True,
    help="Run all registered project plugins (overrides config).",
)
@click.option(
    "--handle-profile",
    help="Override the configured Handle profile for this command.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Continue on transient external failures (e.g., STAC down).",
)
@click.pass_context
def consume(
    ctx: click.Context,
    publish: bool,
    force: bool,
    projects: tuple[str, ...],
    all_projects: bool,
    handle_profile: str | None,
) -> None:
    """Harvest and map Kafka messages, deferring publication by default."""
    ConsumeCommand(
        verbose=ctx.obj["verbose"],
        publish=publish,
        force=force,
        projects=projects,
        all_projects=all_projects,
        handle_profile=handle_profile,
    ).execute()


# command harvest


@cli.command("harvest")
@click.option(
    "--idle-timeout",
    type=click.FloatRange(min=0.1),
    default=5.0,
    show_default=True,
    help="Stop after this many seconds without a Kafka message.",
)
@click.pass_context
def harvest(ctx: click.Context, idle_timeout: float) -> None:
    """Harvest Kafka messages into raw JSONL without mapping."""
    HarvestCommand(verbose=ctx.obj["verbose"], idle_timeout=idle_timeout).execute()


# command map


@cli.command("map")
@click.argument(
    "path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=False
)
@click.option(
    "--date",
    "input_date",
    metavar="DATE",
    callback=_parse_date_selector,
    help=f"Select a dated raw dump ({DATE_FORMATS}; default: last).",
)
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Project plugin to run; repeat to select several (overrides config).",
)
@click.option(
    "--all-projects",
    is_flag=True,
    help="Run all registered project plugins (overrides config).",
)
@click.option(
    "--handle-profile",
    help="Override the configured Handle profile for this command.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Stop after mapping this many dumped messages in total.",
)
@click.option(
    "--offset",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Skip this many dumped messages before mapping.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Continue on transient external failures (e.g., STAC down).",
)
@click.pass_context
def map_messages(
    ctx: click.Context,
    path: tuple[Path, ...],
    input_date: datetime | str | None,
    projects: tuple[str, ...],
    all_projects: bool,
    limit: int | None,
    offset: int,
    force: bool,
    handle_profile: str | None,
) -> None:
    """Map raw message JSONL through plugins into Handle JSONL."""
    MapCommand(
        verbose=ctx.obj["verbose"],
        paths=path,
        input_date=input_date,
        projects=projects,
        all_projects=all_projects,
        limit=limit,
        offset=offset,
        force=force,
        handle_profile=handle_profile,
    ).execute()


# command publish


@cli.command("publish")
@click.argument(
    "path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=False
)
@click.option(
    "--date",
    "input_date",
    metavar="DATE",
    callback=_parse_date_selector,
    help=f"Select a dated Handle file ({DATE_FORMATS}; default: last).",
)
@click.option(
    "--project", help="Validate that every selected Handle belongs to this project."
)
@click.option(
    "--handle-profile",
    help="Override the configured Handle profile for this command.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Stop after attempting this many handles in total.",
)
@click.option(
    "--offset",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Skip this many handles before publishing.",
)
@click.option(
    "--retries",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Retry each transient Handle request this many times.",
)
@click.option(
    "--retry-delay",
    type=click.FloatRange(min=0),
    default=1.0,
    show_default=True,
    help="Initial retry delay in seconds; subsequent delays double.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=1,
    show_default=True,
    help="Publish different handles concurrently; updates to one handle stay ordered.",
)
@click.pass_context
def publish(
    ctx: click.Context,
    path: tuple[Path, ...],
    input_date: datetime | str | None,
    limit: int | None,
    offset: int,
    retries: int,
    retry_delay: float,
    workers: int,
    project: str | None,
    handle_profile: str | None,
) -> None:
    """Publish prepared handles from immutable JSONL FILE_OR_DIRECTORY inputs.

    The source files are never changed. Re-running a file is safe because the
    Handle REST client publishes with overwrite enabled.
    """
    PublishCommand(
        verbose=ctx.obj["verbose"],
        paths=path,
        input_date=input_date,
        limit=limit,
        offset=offset,
        retries=retries,
        retry_delay=retry_delay,
        workers=workers,
        project=project,
        handle_profile=handle_profile,
    ).execute()


# command retry


@cli.command("retry")
@click.argument(
    "path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True
)
@click.option(
    "--delete-after", is_flag=True, help="Delete files after successful retry."
)
@click.option(
    "--publish",
    is_flag=True,
    help="Publish successfully remapped Handles immediately.",
)
@click.option(
    "--handle-profile",
    help="Override the configured Handle profile for this command.",
)
@click.pass_context
def retry(
    ctx: click.Context,
    path: tuple[Path, ...],
    delete_after: bool,
    publish: bool,
    handle_profile: str | None,
) -> None:
    """Retry failed items from failure .jsonl file(s) or directory.

    Accepts multiple arguments:

    \b
      Individual files: retry file1.jsonl file2.jsonl
      Directories: retry outputs/failures/r0/
      Glob patterns: retry outputs/failures/r0/*.jsonl

    Internals: This command uses `RetryRunner` to aggregate results across
    inputs, invoke the processing pipeline, and optionally remove source files
    when `--delete-after` is set and all items succeed.
    """
    RetryCommand(
        verbose=ctx.obj["verbose"],
        paths=path,
        delete_after=delete_after,
        publish=publish,
        handle_profile=handle_profile,
    ).execute()


# command config


@cli.group(name="config")
def config_cmd() -> None:
    """Configuration commands."""


# command config validate


@config_cmd.command("validate")
def config_validate() -> None:
    """Validate the loaded configuration file and defaults."""
    ConfigValidateCommand().execute()


# command config show


@config_cmd.command("show")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["toml", "json"], case_sensitive=False),
    default="toml",
    show_default=True,
    help="Output format.",
)
@click.option("--section", type=str, help="Show only a specific section.")
@click.option("--key", type=str, help="Show a specific key within section.")
def config_show(fmt: str, section: str | None, key: str | None) -> None:
    """Print the effective configuration (defaults + overrides)."""
    ConfigShowCommand(fmt=fmt, section=section, key=key).execute()


@cli.command("top")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Monitoring database (defaults to stats.db_path from configuration).",
)
@click.option("--project", help="Show only one project.")
@click.option("--once", is_flag=True, help="Print one snapshot and exit.")
@click.option(
    "--json", "json_output", is_flag=True, help="Print one JSON snapshot and exit."
)
@click.option(
    "--refresh",
    "refresh_seconds",
    type=click.FloatRange(min=0.1),
    default=2.0,
    show_default=True,
    help="Live refresh interval in seconds.",
)
@click.option(
    "--stale-after",
    "stale_after_seconds",
    type=click.FloatRange(min=0.1),
    help="Mark a running process stale after this many seconds without a heartbeat.",
)
@click.option(
    "--history",
    "history_minutes",
    type=click.FloatRange(min=1),
    help="History window in minutes.",
)
@click.pass_context
def top(
    ctx: click.Context,
    db_path: Path | None,
    project: str | None,
    once: bool,
    json_output: bool,
    refresh_seconds: float,
    stale_after_seconds: float | None,
    history_minutes: float | None,
) -> None:
    """Watch current processing progress, separated by project."""
    stats_config = config.get("stats", {})
    TopCommand(
        verbose=ctx.obj["verbose"],
        db_path=db_path or Path(stats_config.get("db_path", "piddi.db")),
        project=project,
        once=once,
        json_output=json_output,
        refresh_seconds=refresh_seconds,
        stale_after_seconds=(
            stale_after_seconds or stats_config.get("stale_after_seconds", 15)
        ),
        history_minutes=(history_minutes or stats_config.get("history_minutes", 60)),
    ).execute()


if __name__ == "__main__":
    cli()
