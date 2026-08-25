from piddiplatsch.core.project_processing import StacProjectProcessor
from piddiplatsch.plugins.cmip6plus.record import (
    CMIP6PlusDatasetRecord,
    CMIP6PlusFileRecord,
)


class CMIP6PlusProcessor(StacProjectProcessor):
    plugin_name = "cmip6plus"
    dataset_record = CMIP6PlusDatasetRecord
    file_record = CMIP6PlusFileRecord


__all__ = ["CMIP6PlusProcessor"]
