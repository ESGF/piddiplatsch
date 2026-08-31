"""Project-aware routing for the shared ESGF publication stream."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from piddiplatsch.result import ProcessingResult

from .plugin import normalize_project_id
from .registry import get_plugins

logger = logging.getLogger(__name__)


def _project_value(value: Any) -> str | None:
    """Return a scalar project id from common STAC scalar/list representations."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list) and len(value) == 1:
        return _project_value(value[0])
    return None


def extract_project_id(message: dict[str, Any]) -> str | None:
    """Extract and cross-check the project identity from a publication event.

    ``collection_id`` is available for POST and PATCH events and therefore has
    precedence. Full STAC item fields are accepted as fallbacks and checked for
    conflicts when present.
    """
    if not isinstance(message, dict):
        raise ValueError("Publication message must be a JSON object")

    data = message.get("data")
    if not isinstance(data, dict):
        return None
    payload = data.get("payload")
    if not isinstance(payload, dict):
        return None
    item = payload.get("item")
    item = item if isinstance(item, dict) else {}
    properties = item.get("properties")
    properties = properties if isinstance(properties, dict) else {}

    candidates = [
        ("data.payload.collection_id", _project_value(payload.get("collection_id"))),
        ("data.payload.item.collection", _project_value(item.get("collection"))),
        (
            "data.payload.item.properties.project",
            _project_value(properties.get("project")),
        ),
    ]
    present = [(field, value) for field, value in candidates if value is not None]
    if not present:
        return None

    normalized = {normalize_project_id(value) for _, value in present}
    if len(normalized) > 1:
        details = ", ".join(f"{field}={value!r}" for field, value in present)
        raise ValueError(f"Conflicting project identifiers: {details}")
    return present[0][1]


class ProjectRouter:
    """Route each publication event to zero or one selected project plugin."""

    def __init__(
        self,
        projects: Iterable[str] | str,
        *,
        publish: bool = False,
        handle_profile: str | None = None,
        processor_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.plugins = get_plugins(projects)
        kwargs = dict(processor_kwargs or {})
        kwargs.setdefault("publish", publish)
        kwargs.setdefault("handle_profile", handle_profile)
        self.processors = {
            plugin.name: plugin.make_processor(**kwargs) for plugin in self.plugins
        }
        self._plugins_by_project = {
            project_id: plugin
            for plugin in self.plugins
            for project_id in plugin.project_ids
        }
        self._logged_filtered_projects: set[str] = set()
        logger.info("Selected project plugins: %s", ", ".join(self.project_names))

    @property
    def project_names(self) -> tuple[str, ...]:
        return tuple(plugin.name for plugin in self.plugins)

    def __str__(self) -> str:
        return f"projects[{','.join(self.project_names)}]"

    def preflight_check(self, stop_on_transient_skip: bool = True) -> None:
        for plugin in self.plugins:
            processor = self.processors[plugin.name]
            preflight = getattr(processor, "preflight_check", None)
            if callable(preflight):
                preflight(stop_on_transient_skip=stop_on_transient_skip)

    def plugin_name_for(self, message: dict[str, Any]) -> str | None:
        """Return the selected canonical plugin name for an event, if resolvable."""
        project_id = extract_project_id(message)
        plugin = self._plugins_by_project.get(normalize_project_id(project_id or ""))
        return plugin.name if plugin else None

    def process(self, key: str, value: dict[str, Any]) -> ProcessingResult:
        project_id = extract_project_id(value)
        normalized = normalize_project_id(project_id or "")
        plugin = self._plugins_by_project.get(normalized)
        if plugin is None:
            reason = (
                "publication event has no project identifier"
                if not project_id
                else f"project '{project_id}' is not selected"
            )
            filter_identity = normalized or "<missing>"
            if filter_identity not in self._logged_filtered_projects:
                logger.info(
                    "Filtering publication project %s: %s; subsequent messages "
                    "for this project are logged at debug level",
                    filter_identity,
                    reason,
                )
                self._logged_filtered_projects.add(filter_identity)
            logger.debug("Filtered message key=%s: %s", key, reason)
            return ProcessingResult(
                key=key,
                success=True,
                filtered=True,
                filtered_reason=reason,
                project=normalized or None,
            )

        result = self.processors[plugin.name].process(key, value)
        result.project = normalized
        result.plugin = plugin.name
        return result


__all__ = ["ProjectRouter", "extract_project_id"]
