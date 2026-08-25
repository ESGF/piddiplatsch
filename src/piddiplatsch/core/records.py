from abc import ABC, abstractmethod
from functools import cached_property

from pydantic import BaseModel, ValidationError

from piddiplatsch.utils.models import drop_empty


class BaseRecord(ABC):
    def __init__(self, item: dict):
        self.item = item

    @abstractmethod
    def as_handle_model(self) -> BaseModel: ...

    @cached_property
    def model(self) -> BaseModel:
        return self.as_handle_model()

    def validate(self):
        try:
            _ = self.model
        except ValidationError as e:
            raise ValueError(f"Pydantic validation failed: {e}") from e

    def as_record(self) -> dict:
        return drop_empty(self.model.model_dump())

    def as_json(self) -> str:
        return self.model.model_dump_json()

    def __repr__(self) -> str:
        pid = getattr(self, "pid", "UNKNOWN")
        return (
            f"<{self.__class__.__name__} id={self.item.get('id', 'UNKNOWN')} pid={pid}>"
        )

    def __str__(self):
        return self.__repr__()
