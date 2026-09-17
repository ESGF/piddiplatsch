from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec


def make_processor(**kwargs):
    from .processor import CMIP6PlusProcessor

    return CMIP6PlusProcessor(**kwargs)


plugin = PluginSpec(
    name="cmip6plus",
    project_ids=("CMIP6Plus",),
    make_processor=make_processor,
    description="CMIP6Plus data processing plugin",
)
