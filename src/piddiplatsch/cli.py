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
)
from piddiplatsch.config import config

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option("-c", "--config", "config_file", type=click.Path(), help="Path to custom config TOML file.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("-v", "--verbose", is_flag=True, help="Show progress information.")
@click.option(
    "-l",
    "--log",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default="pid.log",
    show_default=True,
    help="Log file path.",
)
@click.pass_context
def cli(ctx: click.Context, config_file: str | None, debug: bool, verbose: bool, log: str) -> None:
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    config.load_user_config(config_file)
    config.configure_logging(debug=debug, log=log)


# command consume


@cli.command()
@click.option("--publish", is_flag=True, help="Also publish mapped Handles immediately; JSONL is always written first.")
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Project plugin to run; repeat to select several (overrides config).",
)
@click.option("--all-projects", is_flag=True, help="Run all registered project plugins (overrides config).")
@click.option("--force", is_flag=True, help="Continue on transient external failures (e.g., STAC down).")
@click.pass_context
def consume(ctx: click.Context, publish: bool, force: bool, projects: tuple[str, ...], all_projects: bool) -> None:
    """Harvest and map Kafka messages, deferring publication by default."""
    ConsumeCommand(
        verbose=ctx.obj["verbose"],
        publish=publish,
        force=force,
        projects=projects,
        all_projects=all_projects,
    ).execute()


# command harvest


@cli.command("harvest")
@click.pass_context
def harvest(ctx: click.Context) -> None:
    """Harvest Kafka messages into raw JSONL without mapping."""
    HarvestCommand(verbose=ctx.obj["verbose"]).execute()


# command map


@cli.command("map")
@click.argument("path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=False)
@click.option(
    "--date",
    "input_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Map the raw dump for this date from the configured output directory.",
)
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Project plugin to run; repeat to select several (overrides config).",
)
@click.option("--all-projects", is_flag=True, help="Run all registered project plugins (overrides config).")
@click.option("--limit", type=click.IntRange(min=1), help="Stop after mapping this many dumped messages in total.")
@click.option(
    "--offset",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Skip this many dumped messages before mapping.",
)
@click.option("--force", is_flag=True, help="Continue on transient external failures (e.g., STAC down).")
@click.pass_context
def map_messages(
    ctx: click.Context,
    path: tuple[Path, ...],
    input_date: datetime | None,
    projects: tuple[str, ...],
    all_projects: bool,
    limit: int | None,
    offset: int,
    force: bool,
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
    ).execute()


# command publish


@cli.command("publish")
@click.argument("path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=False)
@click.option(
    "--date",
    "input_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Publish this project's Handle file for the given date.",
)
@click.option("--project", help="Validate that every selected Handle belongs to this project.")
@click.option("--limit", type=click.IntRange(min=1), help="Stop after attempting this many handles in total.")
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
    input_date: datetime | None,
    limit: int | None,
    offset: int,
    retries: int,
    retry_delay: float,
    workers: int,
    project: str | None,
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
    ).execute()


# command retry


@cli.command("retry")
@click.argument("path", type=click.Path(exists=True, path_type=Path), nargs=-1, required=True)
@click.option("--delete-after", is_flag=True, help="Delete files after successful retry.")
@click.option("--dry-run", is_flag=True, help="Write handles to JSONL without contacting Handle Service.")
@click.pass_context
def retry(ctx: click.Context, path: tuple[Path, ...], delete_after: bool, dry_run: bool) -> None:
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
        dry_run=dry_run,
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


if __name__ == "__main__":
    cli()
