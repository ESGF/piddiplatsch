from __future__ import annotations

from piddiplatsch.core.plugin import PluginSpec


def make_processor(**kwargs):
    from .processor import CordexCMIP6Processor

    return CordexCMIP6Processor(**kwargs)


plugin = PluginSpec(
    name="cordex-cmip6",
    project_ids=("CORDEX-CMIP6",),
    make_processor=make_processor,
    description="CORDEX-CMIP6 data processing plugin",
)
