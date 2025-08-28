from abc import ABC
from datetime import datetime, timezone
from typing_extensions import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Self,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from syft_core import Client as SyftBoxClient

from syft_rds.display_utils.formatter import (
    ANSIPydanticFormatter,
    PydanticFormatter,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


def _utcnow():
    return datetime.now(tz=timezone.utc)


class ItemBase(BaseModel, ABC):
    __schema_name__: str
    __display_formatter__: ClassVar[PydanticFormatter] = ANSIPydanticFormatter()

    uid: UUID = Field(default_factory=uuid4)
    created_by: str | None = None
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)
    client_id: UUID | None = None

    class Config:
        arbitrary_types_allowed: bool = True

    def register_client(self, client: "RDSClient") -> Self:
        self._register_client_id_recursive(client.uid)
        return self

    def _register_client_id_recursive(self, client_id: UUID) -> Self:
        self.client_id = client_id
        for field in self.model_fields.keys():
            field_value = getattr(self, field)
            if isinstance(field_value, ItemBase):
                field_value._register_client_id_recursive(client_id)
        return self

    @property
    def _client(self) -> "RDSClient":
        from syft_rds.client.client_registry import GlobalClientRegistry

        if self.client_id is None:
            raise ValueError("Client ID not set")
        return GlobalClientRegistry.get_client(self.client_id)

    @property
    def _syftbox_client(self) -> SyftBoxClient:
        return self._client._syftbox_client

    @classmethod
    def type_name(cls) -> str:
        return cls.__name__.lower()

    def refresh(self, in_place: bool = True) -> Self:
        new_item = self._client.for_type(type(self)).get(self.uid)
        return self.apply_update(new_item, in_place=in_place)

    def apply_update(
        self, other: Union[Self, "ItemBaseUpdate[Self]"], in_place: bool = True
    ) -> Self:
        """Updates this instance with the provided update instance."""
        if other.uid != self.uid:
            raise ValueError(
                f"Cannot apply update with UID {other.uid} to instance with UID {self.uid}"
            )
        if isinstance(other, type(self)):
            update_dict = other.model_dump()
        elif isinstance(other, ItemBaseUpdate):
            update_target_type = other.get_target_model()
            if other.get_target_model() is not type(self):
                raise ValueError(
                    f"Attempted to apply update for {update_target_type} to {type(self)}"
                )
            update_dict = other.model_dump(exclude_unset=True)
            update_dict["updated_at"] = _utcnow()
        else:
            raise TypeError(
                f"Cannot apply update of type {type(other)} to {type(self)}"
            )

        if in_place:
            for field_name, value in update_dict.items():
                if field_name in self.model_fields:
                    setattr(self, field_name, value)
            return self
        else:
            return self.model_copy(update=update_dict)

    def __str__(self) -> str:
        return self.__display_formatter__.format_str(self)

    def __repr__(self) -> str:
        return self.__display_formatter__.format_repr(self)

    def _repr_html_(self) -> str:
        return self.__display_formatter__.format_html(self)

    def _repr_markdown_(self) -> str:
        return self.__display_formatter__.format_markdown(self)


T = TypeVar("T", bound=ItemBase)


class ItemBaseCreate(BaseModel, Generic[T]):
    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type: ignore

    def to_item(self, extra: Optional[dict[str, Any]] = None) -> T:
        model_cls = self.get_target_model()
        extra = extra or {}
        return model_cls(**self.model_dump(), **extra)


class ItemBaseUpdate(BaseModel, Generic[T]):
    uid: UUID

    @classmethod
    def get_target_model(cls) -> Type[T]:
        return cls.__bases__[0].__pydantic_generic_metadata__["args"][0]  # type: ignore
