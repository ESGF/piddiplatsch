"""Complete Click interface for piddiplatsch."""

import warnings
from datetime import datetime
from pathlib import Path

import click

from piddiplatsch import commands
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


def _parse_range_date(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> datetime | None:
    selected = _parse_date_selector(ctx, param, value)
    if isinstance(selected, str):
        raise click.BadParameter(f"{selected} cannot be used as a range boundary")
    return selected


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
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    help="Increase log detail: -v for INFO, -vv for DEBUG.",
)
@click.option("--debug", is_flag=True, help="Enable DEBUG logging (alias for -vv).")
@click.option(
    "--progress/--no-progress",
    default=True,
    help="Show or hide progress information.",
)
@click.option("-s", "--silent", is_flag=True, help="Hide progress information.")
@click.option(
    "-l",
    "--log",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    help="Override the configured log file path.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    config_file: str | None,
    verbosity: int,
    debug: bool,
    progress: bool,
    silent: bool,
    log: str | None,
) -> None:
    """CLI to interact with Kafka and Handle Service."""
    # HTTP clients can load Brotli on free-threaded Python. Let the interpreter
    # enable its compatibility GIL, but omit this expected diagnostic from CLI
    # output. Scope the filter to this invocation and leave other warnings alone.
    ctx.with_resource(warnings.catch_warnings())
    warnings.filterwarnings(
        "ignore",
        message=(
            r"The global interpreter lock \(GIL\) has been enabled to load module "
            r"'_brotli', which has not declared that it can run safely without the GIL\."
        ),
        category=RuntimeWarning,
    )
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = progress and not silent
    if (
        ctx.get_parameter_source("config_file")
        == click.core.ParameterSource.COMMANDLINE
        and not Path(config_file).is_file()
    ):
        raise click.BadParameter(
            "must name an existing configuration file", param_hint="--config"
        )
    config.load_config_layers(config_file)
    # Inspection/reset must work without opening a possibly unwritable log.
    # Validation reports logging errors through its schema.
    if ctx.invoked_subcommand in {"config", "reset"}:
        return
    try:
        config.configure_logging(verbosity=verbosity, debug=debug, log=log)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command("sync")
@click.option(
    "--project",
    "projects",
    multiple=True,
    help="Restrict mapping and publication to this configured project; repeat to select several.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=None,
    help="Concurrent Handle publication workers (default: [batch].publish_workers).",
)
@click.option(
    "--retries",
    type=click.IntRange(min=0),
    default=None,
    help="Retries per transient Handle request (default: [batch].publish_retries).",
)
@click.option(
    "--force-map",
    is_flag=True,
    help="Map pending items without waiting for the count/time threshold; publishes ready batches.",
)
@click.pass_context
def sync(
    ctx: click.Context,
    projects: tuple[str, ...],
    workers: int | None,
    retries: int | None,
    force_map: bool,
) -> None:
    """Map due catalog changes and publish ready Handle batches."""
    commands.SyncCommand(
        verbose=ctx.obj["verbose"],
        projects=projects,
        workers=workers,
        retries=retries,
        force_map=force_map,
    ).execute()


@cli.command("reset")
@click.option(
    "--project", required=True, help="Project whose processing should be reset."
)
@click.option(
    "--apply",
    is_flag=True,
    help="Archive project output, clear project summaries, and requeue retained work; default is a preview.",
)
def reset(project: str, apply: bool) -> None:
    """Reset project processing, preserving STAC records and Kafka checkpoints.

    Stop harvest and sync before using --apply. This command does not publish.
    """
    commands.ResetCommand(project=project, apply=apply).execute()


# command harvest


@cli.command("harvest")
@click.option(
    "--idle-timeout",
    type=click.FloatRange(min=0),
    default=5.0,
    show_default=True,
    help="Stop after this many seconds without a Kafka message; 0 keeps harvesting continuously.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Stop after harvesting this many Kafka messages.",
)
@click.option(
    "--catalog/--no-catalog",
    default=None,
    help="Maintain project catalogs alongside raw dumps (default: [catalog].enabled).",
)
@click.option(
    "--clean-catalog",
    is_flag=True,
    help="Clear all registered project catalogs and rebuild from retained Kafka history.",
)
@click.option(
    "--clean-project",
    "clean_projects",
    multiple=True,
    help="Clear and rebuild one project's catalog; repeat for several projects.",
)
@click.option(
    "--archive",
    "archives",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Replay a coordinate-bearing JSONL or JSONL.gz archive instead of Kafka; repeat for parts.",
)
@click.option(
    "--ignore-unknown-projects/--stop-on-unknown-projects",
    default=None,
    help="Ignore and archive unknown projects, or stop (default: catalog.ignore_unknown_projects, false).",
)
@click.pass_context
def harvest(
    ctx: click.Context,
    idle_timeout: float,
    limit: int | None,
    catalog: bool | None,
    clean_catalog: bool,
    clean_projects: tuple[str, ...],
    archives: tuple[Path, ...],
    ignore_unknown_projects: bool | None,
) -> None:
    """Harvest Kafka messages into raw JSONL and local catalogs without mapping."""
    commands.HarvestCommand(
        verbose=ctx.obj["verbose"],
        idle_timeout=idle_timeout,
        limit=limit,
        catalog=catalog,
        clean_catalog=clean_catalog,
        clean_projects=clean_projects,
        archives=archives,
        ignore_unknown_projects=ignore_unknown_projects,
    ).execute()


# command map


@cli.command("map")
@click.option(
    "--date",
    "input_date",
    metavar="DATE",
    callback=_parse_date_selector,
    help=f"Select a work creation day in UTC ({DATE_FORMATS}; default: all pending).",
)
@click.option(
    "--from",
    "from_date",
    metavar="DATE",
    callback=_parse_range_date,
    help="First date to map, inclusive (YYYY-MM-DD, today, yesterday, today-N). Requires --to.",
)
@click.option(
    "--to",
    "to_date",
    metavar="DATE",
    callback=_parse_range_date,
    help="Last date to map, inclusive (YYYY-MM-DD, today, yesterday, today-N). Requires --from.",
)
@click.option(
    "--catalog-dir",
    "directory",
    type=click.Path(file_okay=False, path_type=Path),
    help="Read piddi.db from this directory (overrides [catalog].directory).",
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
    help="Stop after mapping this many pending work entries in total.",
)
@click.option(
    "--offset",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Skip this many pending work entries before mapping.",
)
@click.option(
    "--version-history/--no-version-history",
    default=None,
    help="Link to the nearest older catalog version (default: [catalog].version_history).",
)
@click.pass_context
def map_messages(
    ctx: click.Context,
    directory: Path | None,
    input_date: datetime | str | None,
    from_date: datetime | None,
    to_date: datetime | None,
    projects: tuple[str, ...],
    all_projects: bool,
    limit: int | None,
    offset: int,
    version_history: bool | None,
    handle_profile: str | None,
) -> None:
    """Map pending work snapshots into durable Handle JSONL batches."""
    commands.MapCommand(
        verbose=ctx.obj["verbose"],
        directory=directory,
        input_date=input_date,
        from_date=from_date,
        to_date=to_date,
        projects=projects,
        all_projects=all_projects,
        limit=limit,
        offset=offset,
        version_history=version_history,
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
    help=f"Queued publication accepts all batches; file selection ({DATE_FORMATS}; default: all pending).",
)
@click.option(
    "--from",
    "from_date",
    metavar="DATE",
    callback=_parse_range_date,
    help="Reserved for explicit file selection; queued batches must publish in order.",
)
@click.option(
    "--to",
    "to_date",
    metavar="DATE",
    callback=_parse_range_date,
    help="Reserved for explicit file selection; queued batches must publish in order.",
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
    default=4,
    show_default=True,
    help="Publish different handles concurrently; updates to one handle stay ordered.",
)
@click.pass_context
def publish(
    ctx: click.Context,
    path: tuple[Path, ...],
    input_date: datetime | str | None,
    from_date: datetime | None,
    to_date: datetime | None,
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
    commands.PublishCommand(
        verbose=ctx.obj["verbose"],
        paths=path,
        input_date=input_date,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        offset=offset,
        retries=retries,
        retry_delay=retry_delay,
        workers=workers,
        project=project,
        handle_profile=handle_profile,
    ).execute()


@cli.command("retry")
@click.argument("work_ids", nargs=-1, required=True, type=click.IntRange(min=1))
@click.option(
    "--project", required=True, help="Project owning these invalid work entries."
)
def retry(work_ids, project):
    """Retry invalid work when no newer output can be overwritten."""
    from piddiplatsch.catalog.api import get_catalog_directory, open_catalog
    from piddiplatsch.core.registry import get_plugin
    from piddiplatsch.exceptions import CatalogError
    from piddiplatsch.runners.sync import SyncWorker, _worker_lock

    try:
        project = get_plugin(project).name
        with _worker_lock(get_catalog_directory()) as acquired:
            if not acquired:
                raise ValueError(
                    "A batch worker is running; retry when it has finished"
                )
            worker = SyncWorker(projects=(project,), require_configured=False)
            with open_catalog(project) as catalog:
                for identity in work_ids:
                    row = catalog._execute(
                        "SELECT batch_id FROM work_items WHERE project=? AND id=?",
                        (project, identity),
                    ).fetchone()
                    if (
                        row
                        and row[0]
                        and (worker.root / project / "ready" / row[0]).exists()
                    ):
                        raise ValueError(
                            "Finish this saved batch with sync or publish before retrying its invalid work"
                        )
                catalog.retry_invalid(work_ids)
    except (CatalogError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Requeued {len(work_ids)} invalid work entries for {project}.")


@cli.command("maintain")
@click.option(
    "--dry-run", is_flag=True, help="List closed dumps without compressing them."
)
def maintain(dry_run: bool):
    """Compress closed dumps and delete expired published work; never VACUUM."""
    commands.MaintainCommand(dry_run=dry_run).execute()


# command config


@cli.group(name="config")
def config_cmd() -> None:
    """Configuration commands."""


# command config validate


@config_cmd.command("validate")
def config_validate() -> None:
    """Validate the loaded configuration file and defaults."""
    commands.ConfigValidateCommand().execute()


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
@click.option(
    "--show-secrets",
    is_flag=True,
    help="Include credential values (hidden by default).",
)
def config_show(
    fmt: str, section: str | None, key: str | None, show_secrets: bool
) -> None:
    """Print the effective configuration with recognized secrets redacted."""
    commands.ConfigShowCommand(
        fmt=fmt, section=section, key=key, show_secrets=show_secrets
    ).execute()


@config_cmd.command("explain")
@click.option(
    "--project", required=True, help="Project plugin whose settings to resolve."
)
@click.option("--handle-profile", help="Explain using this Handle profile override.")
@click.pass_context
def config_explain(
    ctx: click.Context, project: str, handle_profile: str | None
) -> None:
    """Show resolved project settings and the keys supplying them."""
    commands.ConfigExplainCommand(
        project=project,
        handle_profile=handle_profile,
        log_override=ctx.find_root().params.get("log"),
    ).execute()


@cli.command("top")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Shared piddi.db (defaults to catalog.directory or consumer.output_dir).",
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
@click.pass_context
def top(
    ctx: click.Context,
    db_path: Path | None,
    project: str | None,
    once: bool,
    json_output: bool,
    refresh_seconds: float,
    stale_after_seconds: float | None,
) -> None:
    """Watch current processing progress, separated by project."""
    from piddiplatsch.catalog.api import get_catalog_directory

    stats_config = config.get("stats", {})
    commands.TopCommand(
        verbose=ctx.obj["verbose"],
        db_path=db_path or get_catalog_directory() / "piddi.db",
        project=project,
        once=once,
        json_output=json_output,
        refresh_seconds=refresh_seconds,
        stale_after_seconds=(
            stale_after_seconds or stats_config.get("stale_after_seconds", 15)
        ),
    ).execute()


if __name__ == "__main__":
    cli()
