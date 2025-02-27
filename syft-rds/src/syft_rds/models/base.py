from abc import ABC
from datetime import datetime, timezone
from typing import Any, Generic, Self, Type, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, PrivateAttr

from syft_rds.models.formatter import PydanticFormatterMixin


def _utcnow():
    return datetime.now(tz=timezone.utc)


class BaseSchema(PydanticFormatterMixin, BaseModel, ABC):
    """Base Schema class that all Schema models must inherit from"""

    __schema_name__: str
    uid: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    class Config:
        arbitrary_types_allowed: bool = True

    _client_cache: dict[UUID, "BaseSchema"] = PrivateAttr(default_factory=dict)

    @classmethod
    def type_name(cls) -> str:
        return cls.__name__.lower()

    def clear_cache(self):
        self._client_cache.clear()

    def reload_cache(self, client: Any) -> Self:
        raise NotImplementedError


T = TypeVar("T", bound=BaseSchema)


class BaseSchemaCreate(PydanticFormatterMixin, BaseModel, Generic[T]):
    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type: ignore

    def to_item(self) -> T:
        model_cls = self.get_target_model()
        return model_cls(**self.model_dump())


class BaseSchemaUpdate(PydanticFormatterMixin, BaseModel, Generic[T]):
    uid: UUID

    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type: ignore

    def apply_to(self, item: T) -> T:
        update_dict = self.model_dump(exclude_unset=True)
        updated = item.model_copy(update=update_dict)
        updated.updated_at = _utcnow()
        return updated
