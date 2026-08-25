from piddiplatsch.core.project_processing import StacProjectProcessor
from piddiplatsch.plugins.cordex_cmip6.record import (
    CordexCMIP6DatasetRecord,
    CordexCMIP6FileRecord,
)


class CordexCMIP6Processor(StacProjectProcessor):
    plugin_name = "cordex-cmip6"
    dataset_record = CordexCMIP6DatasetRecord
    file_record = CordexCMIP6FileRecord


__all__ = ["CordexCMIP6Processor"]
