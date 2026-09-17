"""Public exports, loaded on demand to keep configuration commands lightweight."""

from importlib import import_module

_EXPORTS = {  # noqa: RUF067 - lazy public re-exports avoid loading network clients
    "ALLOWED_CHECKSUM_METHODs": "models",
    "HostingNode": "models",
    "strict_mode": "models",
    "PluginSpec": "plugin",
    "normalize_project_id": "plugin",
    "BaseProcessor": "processing",
    "BaseRecord": "records",
    "get_plugin": "registry",
    "get_plugins": "registry",
    "list_plugins": "registry",
    "register_plugin": "registry",
    "ProjectRouter": "routing",
    "extract_project_id": "routing",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{_EXPORTS[name]}", __name__), name)
    globals()[name] = value
    return value
