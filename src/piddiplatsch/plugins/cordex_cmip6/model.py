from piddiplatsch.core.handle_models import DatasetHandleModel, FileHandleModel


class CordexCMIP6DatasetModel(DatasetHandleModel):
    plugin_name = "cordex-cmip6"


class CordexCMIP6FileModel(FileHandleModel):
    pass


__all__ = ["CordexCMIP6DatasetModel", "CordexCMIP6FileModel"]
