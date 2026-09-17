from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec


def make_processor(**kwargs):
    from .processor import CMIP6Processor

    return CMIP6Processor(**kwargs)


plugin = PluginSpec(
    name="cmip6",
    project_ids=("CMIP6",),
    make_processor=make_processor,
    description="CMIP6 data processing plugin",
)
