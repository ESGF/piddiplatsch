from piddiplatsch.core.project_processing import StacProjectProcessor
from piddiplatsch.plugins.cmip6.record import CMIP6DatasetRecord, CMIP6FileRecord


class CMIP6Processor(StacProjectProcessor):
    plugin_name = "cmip6"
    dataset_record = CMIP6DatasetRecord
    file_record = CMIP6FileRecord


__all__ = ["CMIP6Processor"]
