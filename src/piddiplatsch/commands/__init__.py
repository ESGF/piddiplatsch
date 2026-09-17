"""Public exports, loaded on demand to keep configuration commands lightweight."""

from importlib import import_module

_EXPORTS = {  # noqa: RUF067 - lazy public re-exports avoid loading network clients
    "Command": "base",
    "FileBatchCommand": "base",
    "KafkaCommand": "base",
    "ConfigExplainCommand": "config",
    "ConfigShowCommand": "config",
    "ConfigValidateCommand": "config",
    "ConsumeCommand": "consume",
    "HarvestCommand": "harvest",
    "MapCommand": "map",
    "PublishCommand": "publish",
    "RetryCommand": "retry",
    "TopCommand": "top",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{_EXPORTS[name]}", __name__), name)
    globals()[name] = value
    return value
