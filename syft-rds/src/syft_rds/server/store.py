from typing import Generic, TypeVar
from uuid import UUID

from syft_rds.models.base import BaseSchema

T = TypeVar("T", bound=BaseSchema)


class Store(Generic[T]):
    def __init__(self):
        self.items: dict[UUID, T] = {}

    def create(self, item: T) -> T:
        self.items[item.uid] = item
        return item

    def get_by_uid(self, uid: UUID) -> T:
        return self.items[uid]

    def get_all(self) -> list[T]:
        return list(self.items.values())

    def update(self, item: T) -> T:
        self.items[item.uid] = item
        return item
