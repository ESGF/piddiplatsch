from __future__ import annotations

from difflib import get_close_matches
from typing import Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class TransientConfig(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)
    stop_on_skip: bool = True
    retries: int = Field(default=3, ge=0)
    backoff_initial: float = Field(default=0.5, ge=0)
    backoff_max: float = Field(default=5.0, ge=0)
    preflight_stac: bool = False

    @model_validator(mode="after")
    def _check_backoff(self):
        if self.backoff_max < self.backoff_initial:
            raise ValueError("backoff_max must be at least backoff_initial")
        return self


class StatsConfig(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)
    interval_seconds: float = Field(default=10, gt=0)
    summary_interval: int = Field(default=1000, gt=0)
    enable_db: bool = True
    db_path: str = "piddi.db"
    heartbeat_interval_seconds: float = Field(default=5, gt=0)
    sample_interval_seconds: float = Field(default=15, gt=0)
    sample_retention_days: float = Field(default=30, ge=0)
    stale_after_seconds: float = Field(default=15, gt=0)
    history_minutes: float = Field(default=60, gt=0)


class ConsumerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    projects: list[str] | Literal["all"]
    topic: str
    output_dir: str | None = None
    max_errors: int | None = None
    transient: TransientConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _reject_processor_setting(cls, data):
        if isinstance(data, dict) and "processor" in data:
            raise ValueError(
                "[consumer].processor is not supported; use [consumer].projects"
            )
        return data

    @field_validator("projects")
    @classmethod
    def _check_projects(cls, value: list[str] | str | None):
        if isinstance(value, list):
            normalized = [project.strip().casefold() for project in value]
            if not normalized or any(not project for project in normalized):
                raise ValueError("[consumer].projects must not be empty")
            if len(normalized) != len(set(normalized)):
                raise ValueError("[consumer].projects must not contain duplicates")
        return value


class KafkaConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    bootstrap_servers: str = Field(..., alias="bootstrap.servers")
    group_id: str | None = Field(None, alias="group.id")
    auto_offset_reset: str | None = Field(None, alias="auto.offset.reset")
    enable_auto_commit: bool | None = Field(None, alias="enable.auto.commit")
    session_timeout_ms: int | None = Field(None, alias="session.timeout.ms")

    @model_validator(mode="after")
    def _check_plain_credentials(self):
        options = self.model_extra or {}
        protocol = str(options.get("security.protocol", "")).upper()
        mechanism = str(
            options.get("sasl.mechanisms", options.get("sasl.mechanism", "GSSAPI"))
        ).upper()
        if protocol in {"SASL_SSL", "SASL_PLAINTEXT"} and mechanism == "PLAIN":
            missing = [
                key
                for key in ("sasl.username", "sasl.password")
                if not options.get(key)
            ]
            if missing:
                raise ValueError(
                    "Missing required Kafka settings for SASL PLAIN: "
                    + ", ".join(missing)
                )
        return self

    @field_validator("bootstrap_servers")
    @classmethod
    def _check_bootstrap_servers(cls, v: str) -> str:
        def _valid_hostport(token: str) -> bool:
            token = token.strip()
            if not token:
                return False
            try:
                parsed = urlsplit(f"//{token}", allow_fragments=False)
                host = parsed.hostname
                port = parsed.port
                return host is not None and port is not None and 1 <= port <= 65535
            except Exception:
                return False

        invalid = [t for t in v.split(",") if not _valid_hostport(t)]
        if invalid:
            raise ValueError(
                "[kafka].bootstrap.servers must be a comma-separated list of host:port; invalid: "
                + ", ".join(s.strip() for s in invalid)
            )
        return v


class HandleProfileConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Literal["rest", "pyhandle"] | None = None
    server_url: str | None = None
    prefix: str | None = None
    username: str | None = None
    password: str | None = None
    verify_https: bool | None = None
    timeout: float | None = Field(default=None, gt=0)


class HandleConfig(HandleProfileConfig):
    backend: Literal["rest", "pyhandle"] = "rest"
    verify_https: bool = True
    timeout: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _check_publication_requirements(self) -> HandleConfig:
        if not self.server_url:
            raise ValueError("Missing required Handle setting: server_url")
        if not self.prefix:
            raise ValueError("Missing required Handle setting: prefix")
        if not self.username:
            raise ValueError("Missing required Handle setting: username")
        if not self.password:
            raise ValueError("Missing required Handle setting: password")
        return self


class HandlesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default: str
    defaults: HandleProfileConfig = Field(default_factory=HandleProfileConfig)
    profiles: dict[str, HandleProfileConfig]

    @field_validator("default")
    @classmethod
    def _check_default_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("[handles].default must not be empty")
        return value

    @field_validator("profiles")
    @classmethod
    def _check_profiles(cls, value: dict[str, HandleConfig]):
        if not value:
            raise ValueError("[handles.profiles] must not be empty")
        if any(not name.strip() for name in value):
            raise ValueError("Handle profile names must not be empty")
        return value

    @model_validator(mode="after")
    def _check_default_exists(self) -> HandlesConfig:
        if self.default not in self.profiles:
            raise ValueError(
                f"[handles].default references unknown profile {self.default!r}"
            )
        for name in self.profiles:
            try:
                self.resolve(name)
            except ValidationError as exc:
                raise ValueError(
                    f"Handle profile {name!r} is incomplete after applying defaults: {exc}"
                ) from exc
        return self

    def resolve(self, name: str) -> HandleConfig:
        merged = self.defaults.model_dump(exclude_none=True)
        merged.update(self.profiles[name].model_dump(exclude_none=True))
        return HandleConfig.model_validate(merged)


class StacConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: str | None = None
    timeout: float | None = None
    collection: str | None = None


class ElasticsearchConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: str | None = None
    index: str | None = None


class LookupConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Literal["stac", "es"] | None = None
    enabled: bool = False


class SchemaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    strict_mode: bool = True


class LoggingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "WARNING"
    file: str | None = "pid.log"

    @field_validator("level", mode="before")
    @classmethod
    def _normalize_level(cls, value):
        if isinstance(value, str):
            value = value.strip().upper()
            return "WARNING" if value == "WARN" else value
        return value


class ProjectPluginConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    landing_page_url: str | None = None
    handle: str | None = None
    handle_prefix: str | None = Field(default=None, pattern=r"^\S+$")
    stac: StacConfig | None = None


class PluginsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    cmip6: ProjectPluginConfig | None = None
    cmip6plus: ProjectPluginConfig | None = None
    cmip7: ProjectPluginConfig | None = None
    cordex_cmip6: ProjectPluginConfig | None = Field(None, alias="cordex-cmip6")


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    consumer: ConsumerConfig
    kafka: KafkaConfig
    handle: HandleConfig | None = None
    handles: HandlesConfig | None = None
    stac: StacConfig | None = None
    elasticsearch: ElasticsearchConfig | None = None
    lookup: LookupConfig | None = None
    schema_config: SchemaConfig | None = Field(None, alias="schema")
    logging_config: LoggingConfig | None = Field(None, alias="logging")
    plugins: PluginsConfig | None = None
    stats: StatsConfig | None = None

    @model_validator(mode="after")
    def _check_lookup_requirements(self) -> AppConfig:
        if self.lookup and self.lookup.enabled:
            if self.lookup.backend == "stac":
                if not (self.stac and self.stac.base_url):
                    raise ValueError("Missing required setting: [stac].base_url")
            elif self.lookup.backend == "es":
                if not (self.elasticsearch and self.elasticsearch.base_url):
                    raise ValueError(
                        "Missing required setting: [elasticsearch].base_url"
                    )
        return self

    @model_validator(mode="after")
    def _check_project_handle_profiles(self) -> AppConfig:
        # Legacy [handle] overrides named profiles for compatibility.
        if self.handle or not self.plugins:
            return self
        available = set(self.handles.profiles) if self.handles else set()
        for field_name in self.plugins.__class__.model_fields:
            plugin = getattr(self.plugins, field_name)
            if plugin and plugin.handle and plugin.handle not in available:
                raise ValueError(
                    f"[plugins.{field_name}].handle references unknown Handle "
                    f"profile {plugin.handle!r}"
                )
        return self


def _typo_warnings(model: BaseModel, path: str = "") -> list[str]:
    # Kafka options and project extensions are deliberately open-ended.
    if isinstance(model, (KafkaConfig, PluginsConfig, ProjectPluginConfig)):
        return []
    warnings = []
    fields = model.__class__.model_fields
    names = [field.alias or name for name, field in fields.items()]
    for key in model.model_extra or {}:
        matches = get_close_matches(key, names, n=1, cutoff=0.85)
        if matches:
            warnings.append(
                f"Unknown setting {path + key!r}; did you mean {path + matches[0]!r}?"
            )
    for name, field in fields.items():
        value = getattr(model, name)
        if isinstance(value, BaseModel):
            warnings.extend(_typo_warnings(value, f"{path}{field.alias or name}."))
    return warnings


def validate_config(data: dict) -> tuple[list[str], list[str]]:
    """Validate config using Pydantic models and simple cross-checks.

    Returns (errors, warnings). No network calls.
    """
    errors: list[str] = []
    warnings: list[str] = []

    try:
        cfg = AppConfig.model_validate(data)
    except ValidationError as ve:
        for err in ve.errors():
            loc = ".".join(str(x) for x in err.get("loc", []))
            msg = err.get("msg", "invalid configuration")
            errors.append(f"{loc}: {msg}")
        return errors, warnings

    from piddiplatsch.config.config import Config
    from piddiplatsch.core.registry import get_plugins

    resolved = Config()
    resolved.config_data = data
    try:
        selected = get_plugins(cfg.consumer.projects)
    except ValueError as exc:
        return [str(exc)], warnings
    warnings.extend(_typo_warnings(cfg))
    handle_configs = {"legacy": cfg.handle} if cfg.handle else {}
    if cfg.handles and not cfg.handle:
        names = {resolved.get_handle_profile(plugin.name) for plugin in selected}
        unknown = names - cfg.handles.profiles.keys()
        if unknown:
            return [
                f"Unknown selected Handle profile: {name!r}" for name in sorted(unknown)
            ], warnings
        handle_configs.update((name, cfg.handles.resolve(name)) for name in names)
    for name, handle_config in handle_configs.items():
        if (
            handle_config.username == "300:21.TEST/testuser"
            and handle_config.password == "testpass"
        ):
            warnings.append(
                f"Handle profile {name!r} uses demo credentials; do not use in production"
            )
    if cfg.lookup and cfg.lookup.enabled and cfg.lookup.backend == "es":
        if cfg.elasticsearch and not (cfg.elasticsearch.index):
            warnings.append(
                "[elasticsearch].index is not set; some features may be unavailable"
            )

    warnings.extend(
        f"[plugins.{plugin.name}].landing_page_url not set; landing pages may be missing"
        for plugin in selected
        if resolved.get_plugin(plugin.name, "landing_page_url") in (None, "")
    )

    return errors, warnings
