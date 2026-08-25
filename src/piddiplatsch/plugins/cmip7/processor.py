from piddiplatsch.core.project_processing import StacProjectProcessor
from piddiplatsch.plugins.cmip7.record import CMIP7DatasetRecord, CMIP7FileRecord


class CMIP7Processor(StacProjectProcessor):
    plugin_name = "cmip7"
    dataset_record = CMIP7DatasetRecord
    file_record = CMIP7FileRecord


__all__ = ["CMIP7Processor"]
