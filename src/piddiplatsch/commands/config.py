"""Configuration command implementations."""

import json
from dataclasses import dataclass

import click
import toml

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.core.registry import get_plugin


@dataclass(kw_only=True)
class ConfigExplainCommand(Command):
    """Explain project resolution without exposing credential fields."""

    project: str
    handle_profile: str | None = None

    def execute(self) -> None:
        try:
            project = get_plugin(self.project).name
            handle = config.get_handle(project, self.handle_profile)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

        plugin = config.get_plugin(project)
        legacy = config.get(project) or {}

        def plugin_source(key: str) -> str:
            section = project if key in legacy else f"plugins.{project}"
            return f"{section}.{key}"

        def show(label: str, value, source: str) -> None:
            click.echo(f"{label}: {json.dumps(value)}  <- {source}")

        click.echo(f"Project: {project}")
        click.echo("Sources identify configuration keys, not files.")
        for key in ("topic", "output_dir"):
            show(key, config.get("consumer", key), f"consumer.{key}")

        if config.get("handle"):
            profile = None
            click.echo("Handle profile: legacy [handle] (overrides named profiles)")
        else:
            profile = self.handle_profile or config.get_handle_profile(project)
            source = (
                "--handle-profile"
                if self.handle_profile
                else (
                    plugin_source("handle")
                    if plugin.get("handle")
                    else "handles.default"
                )
            )
            show("Handle profile", profile, source)

        profile_values = config.get("handles").get("profiles", {}).get(profile, {})
        for key in ("server_url", "prefix", "backend", "verify_https", "timeout"):
            if key not in handle:
                continue
            if key == "prefix" and plugin.get("handle_prefix") is not None:
                source = plugin_source("handle_prefix")
            elif config.get("handle"):
                source = f"handle.{key}"
            elif key in profile_values:
                source = f"handles.profiles.{profile}.{key}"
            else:
                source = f"handles.defaults.{key}"
            show(f"Handle {key}", handle[key], source)

        stac = config.get_stac(project)
        project_stac = plugin.get("stac") or {}
        for key in ("base_url", "collection", "timeout"):
            source = (
                f"{plugin_source('stac')}.{key}"
                if key in project_stac
                else f"stac.{key}"
            )
            show(f"STAC {key}", stac.get(key), source)
        for key in ("enabled", "backend"):
            show(f"Lookup {key}", config.get("lookup", key), f"lookup.{key}")
        click.echo(
            "Publication: consume writes JSONL; --publish enables immediate delivery."
        )
        click.echo("Deferred publish always uses REST. Credential fields are omitted.")


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
