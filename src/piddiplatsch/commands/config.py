"""Configuration command implementations."""

import json
from dataclasses import dataclass
from pathlib import Path

import click
import toml

from piddiplatsch.commands.base import Command
from piddiplatsch.config import config
from piddiplatsch.config.config import DEFAULT_CONFIG_PATH
from piddiplatsch.config.redaction import redact_config
from piddiplatsch.core.registry import get_plugin


@dataclass(kw_only=True)
class ConfigExplainCommand(Command):
    """Explain project resolution without exposing credential fields."""

    project: str
    handle_profile: str | None = None
    log_override: str | None = None

    def execute(self) -> None:
        try:
            project = get_plugin(self.project).name
            handle = config.get_handle(project, self.handle_profile)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc

        plugin = config.get_plugin(project)
        legacy = config.get(project) or {}
        source_labels = self._source_labels()

        def plugin_source(key: str) -> str:
            section = project if key in legacy else f"plugins.{project}"
            return f"{section}.{key}"

        def show(label: str, value, source: str, path=None) -> None:
            origin = (
                "command line"
                if source.startswith("--")
                else config.get_source(path or tuple(source.split(".")))
            )
            click.echo(
                f"{label}: {json.dumps(value)}  <- {source} ({source_labels.get(origin, origin)})"
            )

        click.echo(f"Project: {project}")
        click.echo("Loaded files (later files override earlier files):")
        for filename in config.loaded_files:
            click.echo(f"  {source_labels[filename]}: {filename}")
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
            path = (
                ("handles", "profiles", profile, key)
                if source == f"handles.profiles.{profile}.{key}"
                else None
            )
            show(f"Handle {key}", handle[key], source, path)

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
        self._explain_paths(show)
        click.echo(
            "Publication: consume writes JSONL; --publish enables immediate delivery."
        )
        click.echo("Deferred publish always uses REST. Credential fields are omitted.")

    def _source_labels(self) -> dict[str, str]:
        labels = {}
        used = set()
        for filename in config.loaded_files:
            base = (
                "defaults"
                if filename == str(DEFAULT_CONFIG_PATH.resolve())
                else Path(filename).name
            )
            label = base
            suffix = 2
            while label in used:
                label = f"{base} [{suffix}]"
                suffix += 1
            labels[filename] = label
            used.add(label)
        return labels

    def _explain_paths(self, show) -> None:
        for label, section, key in (
            ("Output path", "consumer", "output_dir"),
            ("Log path", "logging", "file"),
            ("Database path", "stats", "db_path"),
        ):
            value = config.get(section, key)
            if section == "logging" and self.log_override is not None:
                show(label, str(Path(self.log_override).resolve()), "--log")
                continue
            resolved = (
                str(Path(value).resolve())
                if value
                else "terminal" if section == "logging" else None
            )
            show(label, resolved, f"{section}.{key}")
        click.echo(f"Database enabled: {json.dumps(config.get('stats', 'enable_db'))}")


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
    show_secrets: bool = False

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

        if not self.show_secrets:
            data = redact_config(data)
        output = (
            json.dumps(data, indent=2, sort_keys=True)
            if self.fmt.lower() == "json"
            else toml.dumps(data)
        )
        click.echo(output)
