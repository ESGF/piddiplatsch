"""Static registry for project processing plugins shipped with piddi."""

from __future__ import annotations

from collections.abc import Iterable

from .plugin import PluginSpec, normalize_project_id

_PLUGINS: dict[str, PluginSpec] = {}


def register_plugin(plugin: PluginSpec, *, replace: bool = False) -> None:
    """Register a plugin and reject conflicting names or project identifiers."""
    existing = _PLUGINS.get(plugin.name)
    if existing is not None and not replace:
        raise ValueError(f"Plugin '{plugin.name}' is already registered")

    conflicts = {
        project_id: registered.name
        for registered in _PLUGINS.values()
        if registered.name != plugin.name
        for project_id in plugin.project_ids
        if project_id in registered.project_ids
    }
    if conflicts:
        details = ", ".join(f"{project_id} ({name})" for project_id, name in sorted(conflicts.items()))
        raise ValueError(f"Plugin '{plugin.name}' has project identifiers already registered: {details}")

    _PLUGINS[plugin.name] = plugin


def get_plugin(name: str) -> PluginSpec:
    normalized = normalize_project_id(name)
    try:
        return _PLUGINS[normalized]
    except KeyError as exc:
        available = ", ".join(list_plugins()) or "none"
        raise ValueError(f"Plugin '{name}' not found. Available plugins: {available}") from exc


def get_plugins(names: Iterable[str] | str) -> list[PluginSpec]:
    """Resolve an ordered plugin selection, including the special value ``all``."""
    if isinstance(names, str):
        if normalize_project_id(names) == "all":
            plugins = [_PLUGINS[name] for name in list_plugins()]
            if not plugins:
                raise ValueError("No plugins are registered")
            return plugins
        names = (names,)

    normalized_names = [normalize_project_id(name) for name in names]
    if not normalized_names:
        raise ValueError("At least one project plugin must be selected")
    if "all" in normalized_names:
        raise ValueError("'all' cannot be combined with named project plugins")
    if any(not name for name in normalized_names):
        raise ValueError("Project plugin names must not be empty")
    if len(normalized_names) != len(set(normalized_names)):
        raise ValueError("Project plugin selection contains duplicates")
    return [get_plugin(name) for name in normalized_names]


def list_plugins() -> list[str]:
    return sorted(_PLUGINS)


# Built-ins are imported explicitly. Import failures must stop startup rather
# than leaving a mysteriously incomplete registry.
from piddiplatsch.plugins.cmip6.plugin import plugin as cmip6_plugin  # noqa: E402

register_plugin(cmip6_plugin)
