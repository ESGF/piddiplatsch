from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec

from .processor import CMIP7Processor

plugin = PluginSpec(
    name="cmip7",
    project_ids=("CMIP7",),
    make_processor=lambda **kwargs: CMIP7Processor(**kwargs),
    description="CMIP7 data processing plugin",
)
