from piddiplatsch.core.handle_models import DatasetHandleModel, FileHandleModel


class CMIP7DatasetModel(DatasetHandleModel):
    plugin_name = "cmip7"


class CMIP7FileModel(FileHandleModel):
    pass


__all__ = ["CMIP7DatasetModel", "CMIP7FileModel"]
