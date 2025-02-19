from abc import ABC
from datetime import datetime, timezone
from typing import Generic, Type, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def _utcnow():
    return datetime.now(tz=timezone.utc)


class ItemBase(BaseModel, ABC):
    uid: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    @classmethod
    def type_name(cls) -> str:
        return cls.__name__.lower()


T = TypeVar("T", bound=ItemBase)


class ItemBaseCreate(BaseModel, Generic[T]):
    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__orig_bases__[0].__args__[0]

    def to_item(self) -> T:
        model_cls = self.get_target_model()
        return model_cls(**self.model_dump())


class ItemBaseUpdate(BaseModel, Generic[T]):
    uid: UUID

    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__orig_bases__[0].__args__[0]

    def update_item(self, item: T) -> T:
        update_dict = self.model_dump(exclude_unset=True)
        updated = item.model_copy(update=update_dict)
        updated.updated_at = _utcnow()
        return updated
