from piddiplatsch.core.handle_models import (
    BaseHandleModel,
    DatasetHandleModel,
    FileHandleModel,
)


BaseCMIP6Model = BaseHandleModel


class CMIP6DatasetModel(DatasetHandleModel):
    plugin_name = "cmip6"


class CMIP6FileModel(FileHandleModel):
    pass


__all__ = ["BaseCMIP6Model", "CMIP6DatasetModel", "CMIP6FileModel"]
