import json
from pathlib import Path

import click
import toml

from piddiplatsch.config import config
from piddiplatsch.consumer import configured_projects, start_consumer
from piddiplatsch.persist.retry import RetryRunner

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
@click.option("--dump", is_flag=True, help="Dump all consumed messages to JSONL files.")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Do not publish to handle server; write handles to JSONL only.",
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
def consume(ctx, dump, dry_run, force, projects, all_projects):
    """Start the Kafka consumer."""
    if projects and all_projects:
        raise click.UsageError("--project cannot be combined with --all-projects")
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    selection = "all" if all_projects else (projects or None)
    start_consumer(
        topic,
        kafka_cfg,
        projects=selection,
        dump_messages=dump,
        verbose=ctx.obj["verbose"],
        dry_run=dry_run,
        force=force,
    )


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
    failure_dir = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"

    # Define progress callback for verbose mode
    def show_progress(file, idx, total, result):
        if verbose:
            click.echo(f"[{idx}/{total}] {file.name}: ", nl=False)
            if result.total > 0:
                click.echo(f"{result.succeeded}/{result.total} succeeded" + (f", {result.failed} failed" if result.failed > 0 else ""))
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
        click.echo(f"  ⚠️  {result.failed} items failed again ({result.success_rate:.1f}% success rate)")
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
