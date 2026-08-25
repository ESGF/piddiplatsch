from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec

from .processor import CMIP6PlusProcessor

plugin = PluginSpec(
    name="cmip6plus",
    project_ids=("CMIP6Plus",),
    make_processor=lambda **kwargs: CMIP6PlusProcessor(**kwargs),
    description="CMIP6Plus data processing plugin",
)
