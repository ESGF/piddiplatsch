"""Retry command implementation."""

from dataclasses import dataclass
from pathlib import Path

import click

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.consumer import configured_projects
from piddiplatsch.persist.retry import RetryRunner


@dataclass(kw_only=True)
class RetryCommand(Command):
    """Retry persisted processing failures."""

    paths: tuple[Path, ...]
    delete_after: bool = False
    dry_run: bool = False

    def execute(self) -> None:
        failure_dir = Path(config.get("consumer", {}).get("output_dir", "outputs")) / "failures"

        def show_progress(file, idx, total, result):
            if self.verbose:
                click.echo(f"[{idx}/{total}] {file.name}: ", nl=False)
                if result.total > 0:
                    click.echo(f"{result.succeeded}/{result.total} succeeded" + (f", {result.failed} failed" if result.failed > 0 else ""))
                else:
                    click.echo("(empty)")

        runner = RetryRunner(
            projects=configured_projects(),
            failure_dir=failure_dir,
            delete_after=self.delete_after,
            dry_run=self.dry_run,
        )
        result = runner.run_batch(
            self.paths,
            verbose=self.verbose,
            progress_callback=show_progress if self.verbose else None,
        )

        if result.total == 0:
            click.echo("No retry files found.")
            return

        click.echo(f"\nTotal: {result.succeeded}/{result.total} succeeded")
        if result.failed > 0:
            click.echo(f"  ⚠️  {result.failed} items failed again ({result.success_rate:.1f}% success rate)")
            if result.skipped:
                click.echo(f"  {result.skipped} item(s) were skipped and remain retryable")
            if result.filtered:
                click.echo(f"  {result.filtered} item(s) did not match a selected project plugin")
            for error in result.errors:
                click.echo(f"  - {error}")
            if result.failure_files:
                click.echo("  New failures saved to:")
                for failure_file in sorted(result.failure_files):
                    click.echo(f"    - {failure_file.relative_to(failure_dir)}")
        else:
            click.echo("  ✓ All items processed successfully!")
