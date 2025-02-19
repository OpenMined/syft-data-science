from typing import Generic, Literal, TypeVar
from uuid import UUID

from syft_rds.models.base import ItemBase

T = TypeVar("T", bound=ItemBase)
SortOrder = Literal["asc", "desc"]


class Store(Generic[T]):
    def __init__(self):
        self.items: dict[str, T] = {}

    def create(self, item: T) -> T:
        self.items[item.uid] = item
        return item

    def get(self, uid: UUID) -> T:
        return self.items[uid]

    def get_all(
        self,
        order_by: str,
        order: SortOrder,
        limit: int,
        offset: int,
    ) -> list[T]:
        return list(self.items.values())
