from piddiplatsch.core.handle_models import DatasetHandleModel, FileHandleModel


class CMIP6PlusDatasetModel(DatasetHandleModel):
    plugin_name = "cmip6plus"


class CMIP6PlusFileModel(FileHandleModel):
    pass


__all__ = ["CMIP6PlusDatasetModel", "CMIP6PlusFileModel"]
