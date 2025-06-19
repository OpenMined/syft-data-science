from typing import Any, Generic, Literal, Optional, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field

T = TypeVar("T", bound=BaseModel)


class ItemList(BaseModel, Generic[T]):
    # Used by get_all endpoints
    items: list[T]


class GetAllRequest(BaseModel):
    limit: Optional[int] = None
    offset: int = 0
    filters: dict[str, Any] = Field(default_factory=dict)
    order_by: Optional[str] = "created_at"
    sort_order: Literal["desc", "asc"] = "desc"


class GetOneRequest(BaseModel):
    uid: UUID | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
