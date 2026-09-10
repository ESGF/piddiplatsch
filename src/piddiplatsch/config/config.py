import logging
from logging.handlers import WatchedFileHandler
from pathlib import Path

import toml
from rich.logging import RichHandler

from piddiplatsch.config.schema import validate_config

DEFAULT_CONFIG_PATH = Path(__file__).parent / "default.toml"
DEFAULT_SITE_CONFIG_PATH = Path("/etc/piddi/piddi.toml")


class Config:
    def __init__(self):
        self.config_data = self._load_toml(DEFAULT_CONFIG_PATH)

    def _load_toml(self, path: Path):
        if path.exists():
            return toml.load(path)
        return {}

    # --- Test / internal helper to override config values ---
    def _set(self, section: str, key: str | None, value):
        """
        Private setter for testing or dynamic overrides.
        Example:
            config._set("cmip6", "max_parts", 5)
            config._set("schema", None, {"strict_mode": True})
        """
        if key is None:
            # Replace the entire section
            self.config_data[section] = value
        else:
            if section not in self.config_data:
                self.config_data[section] = {}
            self.config_data[section][key] = value

    def load_user_config(self, user_config_path: str | None):
        if user_config_path:
            user_path = Path(user_config_path)
            if user_path.exists():
                user_data = self._load_toml(user_path)
                self._merge_dicts(self.config_data, user_data)

    def load_config_layers(
        self,
        local_config_path: str | Path | None,
        site_config_path: str | Path = DEFAULT_SITE_CONFIG_PATH,
    ) -> None:
        """Merge optional site and local configuration over packaged defaults.

        The site configuration is loaded first. The local configuration (or a
        path explicitly selected with ``--config``) is loaded last and thus has
        the highest precedence. Missing optional files are ignored.
        """
        paths = [Path(site_config_path)]
        if local_config_path is not None:
            paths.append(Path(local_config_path))

        loaded: set[Path] = set()
        for path in paths:
            normalized = path.resolve()
            if normalized in loaded:
                continue
            loaded.add(normalized)
            self.load_user_config(str(path))

    def _merge_dicts(self, base, override):
        for key, value in override.items():
            if isinstance(value, dict) and key in base:
                self._merge_dicts(base[key], value)
            else:
                base[key] = value

    def get(self, section: str, key: str | None = None, fallback=None):
        cfg = self.config_data.get(section, {})
        if key:
            value = cfg.get(key, fallback)
        else:
            value = cfg
        return value

    def get_plugin(self, name: str, key: str | None = None, fallback=None):
        """Return plugin-scoped config with backwards compatibility.

        Merges `[plugins.<name>]` with legacy top-level `[<name>]`,
        giving precedence to the legacy top-level for test overrides.
        """
        plugins = self.config_data.get("plugins", {})
        merged: dict = dict(plugins.get(name, {}) or {})
        legacy = self.config_data.get(name, {}) or {}
        # Let legacy (tests/user overrides) take precedence over defaults
        merged.update(legacy)
        if key:
            return merged.get(key, fallback)
        return merged

    def get_stac(self, project: str | None = None) -> dict:
        """Resolve global STAC settings with optional project overrides."""
        merged = dict(self.config_data.get("stac", {}) or {})
        if project:
            project_stac = self.get_plugin(project, "stac", {}) or {}
            if isinstance(project_stac, dict):
                merged.update(project_stac)
        return merged

    def get_handle_profile(self, project: str | None = None) -> str | None:
        """Return the named Handle profile selected for a project.

        A legacy ``[handle]`` section has no profile name and takes precedence
        when present so existing site configuration keeps working during the
        migration to ``[handles.profiles.*]``.
        """
        if self.config_data.get("handle"):
            return None
        handles = self.config_data.get("handles", {}) or {}
        selected = self.get_plugin(project, "handle") if project else None
        return selected or handles.get("default")

    def get_handle(
        self, project: str | None = None, profile: str | None = None
    ) -> dict:
        """Resolve one Handle server configuration.

        Projects select profiles with ``plugins.<project>.handle``. Callers may
        explicitly select a profile, otherwise ``handles.default`` is used.
        """
        legacy = self.config_data.get("handle")
        if legacy:
            return legacy

        handles = self.config_data.get("handles", {}) or {}
        selected = profile or self.get_handle_profile(project)
        if not selected:
            raise ValueError(
                "No Handle profile selected; set [handles].default or "
                "[plugins.<project>].handle"
            )
        profiles = handles.get("profiles", {}) or {}
        try:
            profile_config = profiles[selected]
        except KeyError as exc:
            raise ValueError(f"Unknown Handle profile {selected!r}") from exc
        if not isinstance(profile_config, dict):
            raise ValueError(f"Handle profile {selected!r} must be a table")
        defaults = handles.get("defaults", {}) or {}
        if not isinstance(defaults, dict):
            raise ValueError("[handles.defaults] must be a table")
        return {**defaults, **profile_config}

    def configure_logging(
        self,
        verbosity: int = 0,
        debug: bool = False,
        log: str | None = None,
    ) -> None:
        """Configure logging from config, with CLI verbosity overrides."""
        logging_config = self.get("logging", {}) or {}
        configured_level = str(logging_config.get("level", "WARNING")).strip().upper()
        if configured_level == "WARN":
            configured_level = "WARNING"
        if configured_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid logging level: {configured_level}")

        if debug or verbosity >= 2:
            log_level = logging.DEBUG
        elif verbosity == 1:
            log_level = logging.INFO
        else:
            log_level = getattr(logging, configured_level)

        log_path = log if log is not None else logging_config.get("file", "pid.log")

        handlers = []

        if not log_path:
            console = True
        else:
            console = False

        if console:
            handlers.append(RichHandler(rich_tracebacks=True))
        else:
            handlers.append(WatchedFileHandler(log_path, encoding="utf-8"))

        logging.basicConfig(
            level=log_level,
            format=(
                "%(message)s"
                if console
                else "%(asctime)s - %(levelname)s - %(message)s"
            ),
            datefmt="[%X]",
            handlers=handlers,
            force=True,
        )

    def validate(self) -> tuple[list[str], list[str]]:
        """Validate the loaded configuration using Pydantic models."""
        return validate_config(self.config_data)


# singleton instance
config = Config()
