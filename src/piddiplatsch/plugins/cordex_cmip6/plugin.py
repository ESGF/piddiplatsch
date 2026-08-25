from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec

from .processor import CordexCMIP6Processor

plugin = PluginSpec(
    name="cordex-cmip6",
    project_ids=("CORDEX-CMIP6",),
    make_processor=lambda **kwargs: CordexCMIP6Processor(**kwargs),
    description="CORDEX-CMIP6 data processing plugin",
)
