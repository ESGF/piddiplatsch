import json
from datetime import datetime
from pathlib import Path

import click
import toml
from tqdm import tqdm

from piddiplatsch.config import config
from piddiplatsch.consumer import (
    HarvestProcessor,
    configured_projects,
    map_dump_files,
    start_consumer,
)
from piddiplatsch.core.plugin import normalize_project_id
from piddiplatsch.exceptions import JsonlReadError
from piddiplatsch.handles.publish import HandlePublisher
from piddiplatsch.persist.retry import RetryRunner
from piddiplatsch.result import PublishResult

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option(
    "-c",
    "--config",
    "config_file",
    type=click.Path(),
    help="Path to custom config TOML file.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("-v", "--verbose", is_flag=True, help="Show progress bar.")
@click.option(
    "-l",
    "--log",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default="pid.log",
    show_default=True,
    help="Log file path.",
)
@click.pass_context
def cli(ctx, config_file, debug, verbose, log):
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    config.load_user_config(config_file)
    config.configure_logging(debug=debug, log=log)


# consume command


@cli.command()
@click.option(
    "--publish",
    is_flag=True,
    help="Also publish mapped Handles immediately; JSONL is always written first.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Continue on transient external failures (e.g., STAC down).",
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
@click.pass_context
def consume(ctx, publish, force, projects, all_projects):
    """Harvest and map Kafka messages, deferring publication by default."""
    if projects and all_projects:
        raise click.UsageError("--project cannot be combined with --all-projects")
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    selection = "all" if all_projects else (projects or None)
    start_consumer(
        topic,
        kafka_cfg,
        projects=selection,
        dump_messages=True,
        verbose=ctx.obj["verbose"],
        dry_run=not publish,
        force=force,
    )


@cli.command("harvest")
@click.pass_context
def harvest(ctx):
    """Harvest Kafka messages into raw JSONL without mapping."""
    start_consumer(
        config.get("consumer", "topic"),
        config.get("kafka"),
        processor=HarvestProcessor(),
        dump_messages=True,
        verbose=ctx.obj["verbose"],
        force=True,
    )


@cli.command("map")
@click.argument(
    "path",
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
    required=False,
)
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
@click.option(
    "--all-projects",
    is_flag=True,
    help="Run all registered project plugins (overrides config).",
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
    ctx,
    path: tuple[Path, ...],
    projects: tuple[str, ...],
    all_projects: bool,
    limit: int | None,
    offset: int,
    force: bool,
    input_date: datetime | None,
):
    """Map raw message JSONL through plugins into Handle JSONL."""
    if projects and all_projects:
        raise click.UsageError("--project cannot be combined with --all-projects")
    path = _resolve_map_paths(path, input_date)
    selection = "all" if all_projects else (projects or None)
    try:
        result = map_dump_files(
            path,
            projects=selection,
            limit=limit,
            offset=offset,
            force=force,
            verbose=ctx.obj["verbose"],
        )
    except (JsonlReadError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if result.total == 0:
        click.echo("No dumped messages found.")
        return
    click.echo(f"Mapped {result.succeeded}/{result.total} dumped messages.")
    if result.filtered:
        click.echo(f"Filtered by project selection: {result.filtered}")
    if result.skipped:
        click.echo(f"Skipped: {result.skipped}")
    if result.failed:
        click.echo(f"Failed: {result.failed}")
        raise click.exceptions.Exit(1)


# publish command


@cli.command("publish")
@click.argument(
    "path",
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
    required=False,
)
@click.option(
    "--date",
    "input_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Publish this project's Handle file for the given date.",
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
@click.option(
    "--project",
    help="Validate that every selected Handle belongs to this project.",
)
@click.pass_context
def publish(
    ctx,
    path: tuple[Path, ...],
    limit: int | None,
    offset: int,
    retries: int,
    retry_delay: float,
    workers: int,
    project: str | None,
    input_date: datetime | None,
):
    """Publish prepared handles from immutable JSONL FILE_OR_DIRECTORY inputs.

    The source files are never changed. Re-running a file is safe because the
    Handle REST client publishes with overwrite enabled.
    """
    path = _resolve_publish_paths(path, input_date, project)
    verbose = ctx.obj.get("verbose", False)
    last_handle_position = offset
    progress_bar = None
    progress_succeeded = 0
    progress_failed = 0

    def show_progress(index, total, handle, error):
        nonlocal last_handle_position, progress_bar, progress_succeeded, progress_failed
        last_handle_position = max(last_handle_position, offset + index)
        if not verbose:
            return
        if progress_bar is None:
            progress_label = (
                f"publish {project} handles" if project else "publish handles"
            )
            progress_bar = tqdm(
                total=total,
                desc=f"{progress_label} {offset + 1}-{offset + total}",
                unit="handle",
                dynamic_ncols=True,
            )
        if error is None:
            progress_succeeded += 1
        else:
            progress_failed += 1
        progress_bar.set_postfix(
            position=last_handle_position,
            ok=progress_succeeded,
            failed=progress_failed,
        )
        progress_bar.update(1)

    try:
        try:
            result = HandlePublisher().run(
                path,
                limit=limit,
                offset=offset,
                retries=retries,
                retry_delay=retry_delay,
                workers=workers,
                progress_callback=show_progress,
                project=project,
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
    finally:
        if progress_bar is not None:
            progress_bar.close()

    if result.total == 0:
        click.echo("No handles found.")
        return

    click.echo(f"Published {result.succeeded}/{result.total} handles.")
    _show_publish_projects(result)
    if result.result_file is not None:
        click.echo(f"Publication results: {result.result_file}")
    if last_handle_position > offset:
        click.echo(f"Processed handles: {offset + 1}-{last_handle_position}.")
    if limit is not None and result.total == limit:
        click.echo(f"Stopped after reaching the limit of {limit} handles.")
    if result.retry_attempts:
        click.echo(f"Retry attempts: {result.retry_attempts}")
    if result.failed:
        click.echo(f"Failed: {result.failed}")
        for error in result.errors:
            click.echo(f"  - {error}")
        raise click.exceptions.Exit(1)


def _show_publish_projects(result: PublishResult) -> None:
    if not result.projects:
        return
    click.echo("Projects:")
    for project_name, project_result in sorted(result.projects.items()):
        click.echo(
            f"  {project_name}: {project_result.succeeded}/{project_result.total} "
            f"published, {project_result.failed} failed"
        )


def _resolve_map_paths(
    paths: tuple[Path, ...], input_date: datetime | None
) -> tuple[Path, ...]:
    if paths and input_date is not None:
        raise click.UsageError("PATH cannot be combined with --date")
    if paths:
        return paths
    if input_date is None:
        raise click.UsageError("Provide PATH or --date")
    output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
    path = output_dir / "dump" / f"dump_messages_{input_date.date().isoformat()}.jsonl"
    if not path.is_file():
        raise click.ClickException(f"Raw dump does not exist: {path}")
    return (path,)


def _resolve_publish_paths(
    paths: tuple[Path, ...],
    input_date: datetime | None,
    project: str | None,
) -> tuple[Path, ...]:
    if paths and input_date is not None:
        raise click.UsageError("PATH cannot be combined with --date")
    if paths:
        return paths
    if input_date is None:
        raise click.UsageError("Provide PATH or --date")
    project_name = normalize_project_id(project or "")
    if not project_name:
        raise click.UsageError("--date requires --project")
    output_dir = Path(config.get("consumer", {}).get("output_dir", "outputs"))
    path = (
        output_dir
        / project_name
        / "handles"
        / f"handles_{input_date.date().isoformat()}.jsonl"
    )
    if not path.is_file():
        raise click.ClickException(f"Handle file does not exist: {path}")
    return (path,)


# retry command


@cli.command("retry")
@click.argument(
    "path",
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
    required=True,
)
@click.option(
    "--delete-after",
    is_flag=True,
    help="Delete files after successful retry.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Write handles to JSONL without contacting Handle Service.",
)
@click.pass_context
def retry(ctx, path: tuple[Path, ...], delete_after: bool, dry_run: bool):
    """Retry failed items from failure .jsonl file(s) or directory.

    Accepts multiple arguments:
    - Individual files: retry file1.jsonl file2.jsonl
    - Directories: retry outputs/failures/r0/
    - Glob patterns: retry outputs/failures/r0/*.jsonl

    Internals: This command uses `RetryRunner` to aggregate results across
    inputs, invoke the processing pipeline, and optionally remove source files
    when `--delete-after` is set and all items succeed.
    """
    projects = configured_projects()
    verbose = ctx.obj.get("verbose", False)
    failure_dir = (
        Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"
    )

    # Define progress callback for verbose mode
    def show_progress(file, idx, total, result):
        if verbose:
            click.echo(f"[{idx}/{total}] {file.name}: ", nl=False)
            if result.total > 0:
                click.echo(
                    f"{result.succeeded}/{result.total} succeeded"
                    + (f", {result.failed} failed" if result.failed > 0 else "")
                )
            else:
                click.echo("(empty)")

    runner = RetryRunner(
        projects=projects,
        failure_dir=failure_dir,
        delete_after=delete_after,
        dry_run=dry_run,
    )
    result = runner.run_batch(
        path,
        verbose=verbose,
        progress_callback=show_progress if verbose else None,
    )

    if result.total == 0:
        click.echo("No retry files found.")
        return

    # Show overall summary
    click.echo(f"\nTotal: {result.succeeded}/{result.total} succeeded")
    if result.failed > 0:
        click.echo(
            f"  ⚠️  {result.failed} items failed again ({result.success_rate:.1f}% success rate)"
        )
        if result.skipped:
            click.echo(f"  {result.skipped} item(s) were skipped and remain retryable")
        if result.filtered:
            click.echo(
                f"  {result.filtered} item(s) did not match a selected project plugin"
            )
        for error in result.errors:
            click.echo(f"  - {error}")
        if result.failure_files:
            click.echo("  New failures saved to:")
            for failure_file in sorted(result.failure_files):
                rel_path = failure_file.relative_to(failure_dir)
                click.echo(f"    - {rel_path}")
    else:
        click.echo("  ✓ All items processed successfully!")


# config commands


@cli.group(name="config")
def config_cmd():
    """Configuration commands."""
    pass


@config_cmd.command("validate")
def config_validate():
    """Validate the loaded configuration file and defaults."""
    errors, warnings = config.validate()
    if warnings:
        click.echo("Warnings:")
        for w in warnings:
            click.echo(f"  - {w}")
    if errors:
        click.echo("Errors:")
        for e in errors:
            click.echo(f"  - {e}")
        # Non-zero exit if invalid
        raise SystemExit(1)
    click.echo("✓ Configuration is valid")


@config_cmd.command("show")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["toml", "json"], case_sensitive=False),
    default="toml",
    show_default=True,
    help="Output format",
)
@click.option("--section", type=str, help="Show only a specific section")
@click.option("--key", type=str, help="Show a specific key within section")
def config_show(fmt: str, section: str | None, key: str | None):
    """Print the effective configuration (defaults + overrides)."""
    # Build the view of config to render
    if section and key:
        value = config.get(section, key)
        if value is None:
            raise SystemExit(f"Not found: [{section}] {key}")
        data = {section: {key: value}}
    elif section:
        sect = config.get(section)
        if not sect:
            raise SystemExit(f"Not found: [{section}]")
        data = {section: sect}
    else:
        data = config.config_data

    # Render
    if fmt.lower() == "json":
        click.echo(json.dumps(data, indent=2, sort_keys=True))
    else:
        click.echo(toml.dumps(data))


if __name__ == "__main__":
    cli()
