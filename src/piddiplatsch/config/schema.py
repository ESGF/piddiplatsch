from __future__ import annotations

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


class ConsumerConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    projects: list[str] | Literal["all"]
    topic: str
    output_dir: str | None = None
    max_errors: int | None = None

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


class HandleConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Literal["rest", "pyhandle", "jsonl"] | None = None
    server_url: str | None = None
    prefix: str | None = None
    username: str | None = None
    password: str | None = None
    verify_https: bool = True
    timeout: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _check_publication_requirements(self) -> HandleConfig:
        if self.backend in ("rest", "pyhandle"):
            if not self.server_url:
                raise ValueError("Missing required setting: [handle].server_url")
            if not self.prefix:
                raise ValueError("Missing required setting: [handle].prefix")
            if not self.username:
                raise ValueError("Missing required setting: [handle].username")
            if not self.password:
                raise ValueError("Missing required setting: [handle].password")
        return self


class StacConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: str | None = None


class ElasticsearchConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    base_url: str | None = None
    index: str | None = None


class LookupConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    backend: Literal["stac", "es"] | None = None
    enabled: bool = True


class SchemaConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    strict_mode: bool = True


class ProjectPluginConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    landing_page_url: str | None = None


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
    stac: StacConfig | None = None
    elasticsearch: ElasticsearchConfig | None = None
    lookup: LookupConfig | None = None
    schema_config: SchemaConfig | None = Field(None, alias="schema")
    plugins: PluginsConfig | None = None

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

    # warnings
    if (
        cfg.handle
        and cfg.handle.backend in ("rest", "pyhandle")
        and (
            cfg.handle.username == "300:21.TEST/testuser"
            and cfg.handle.password == "testpass"
        )
    ):
        warnings.append("[handle] demo credentials detected; do not use in production")
    if cfg.lookup and cfg.lookup.enabled and cfg.lookup.backend == "es":
        if cfg.elasticsearch and not (cfg.elasticsearch.index):
            warnings.append(
                "[elasticsearch].index is not set; some features may be unavailable"
            )

    # schema strict_mode type handled by Pydantic; add no-op

    # plugins cmip6 hint
    lp = (
        cfg.plugins.cmip6.landing_page_url
        if (cfg.plugins and cfg.plugins.cmip6)
        else None
    )
    if lp in (None, ""):
        warnings.append(
            "[plugins.cmip6].landing_page_url not set; landing pages may be missing"
        )

    return errors, warnings
