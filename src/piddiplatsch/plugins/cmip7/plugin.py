from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec


def make_processor(**kwargs):
    from .processor import CMIP7Processor

    return CMIP7Processor(**kwargs)


plugin = PluginSpec(
    name="cmip7",
    project_ids=("CMIP7",),
    make_processor=make_processor,
    description="CMIP7 data processing plugin",
)
