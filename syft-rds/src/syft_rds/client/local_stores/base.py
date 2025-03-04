from typing import TYPE_CHECKING, ClassVar, Generic, Type, TypeVar

from syft_core import Client as SyftBoxClient

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
)
from syft_rds.store import RDSStore

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig

T = TypeVar("T", bound=BaseSchema)
CreateT = TypeVar("CreateT", bound=BaseSchemaCreate)
UpdateT = TypeVar("UpdateT", bound=BaseSchemaUpdate)


class LocalStoreModule:
    SCHEMA: ClassVar[Type[T]]

    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        if not hasattr(self, "SCHEMA"):
            raise ValueError(f"{self.__class__.__name__} must define a SCHEMA.")

        self.config = config
        self.syftbox_client = syftbox_client
        self.store = RDSStore(
            schema=self.SCHEMA,
            client=self.syftbox_client,
            datasite=self.config.host,
        )


class CRUDLocalStore(LocalStoreModule, Generic[T, CreateT, UpdateT]):
    def create(self, item: CreateT) -> T:
        raise NotImplementedError

    def get_one(self, request: GetOneRequest) -> T:
        raise NotImplementedError

    def get_all(self, request: GetAllRequest) -> list[T]:
        raise NotImplementedError

    def update(self, item: UpdateT) -> T:
        raise NotImplementedError
