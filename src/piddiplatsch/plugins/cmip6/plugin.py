from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec

from .processor import CMIP6Processor

plugin = PluginSpec(
    name="cmip6",
    project_ids=("CMIP6",),
    make_processor=lambda **kwargs: CMIP6Processor(**kwargs),
    description="CMIP6 data processing plugin",
)
