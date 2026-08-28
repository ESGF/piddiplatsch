"""Configuration command implementations."""

import json
from dataclasses import dataclass

import click
import toml

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config


@dataclass(kw_only=True)
class ConfigValidateCommand(Command):
    """Validate the effective configuration."""

    def execute(self) -> None:
        errors, warnings = config.validate()
        if warnings:
            click.echo("Warnings:")
            for warning in warnings:
                click.echo(f"  - {warning}")
        if errors:
            click.echo("Errors:")
            for error in errors:
                click.echo(f"  - {error}")
            raise SystemExit(1)
        click.echo("✓ Configuration is valid")


@dataclass(kw_only=True)
class ConfigShowCommand(Command):
    """Render the effective configuration."""

    fmt: str = "toml"
    section: str | None = None
    key: str | None = None

    def execute(self) -> None:
        if self.key and not self.section:
            raise click.UsageError("--key requires --section")
        if self.section and self.key:
            value = config.get(self.section, self.key)
            if value is None:
                raise SystemExit(f"Not found: [{self.section}] {self.key}")
            data = {self.section: {self.key: value}}
        elif self.section:
            selected = config.get(self.section)
            if not selected:
                raise SystemExit(f"Not found: [{self.section}]")
            data = {self.section: selected}
        else:
            data = config.config_data

        output = (
            json.dumps(data, indent=2, sort_keys=True)
            if self.fmt.lower() == "json"
            else toml.dumps(data)
        )
        click.echo(output)
