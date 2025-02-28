from typing import TYPE_CHECKING
from typing import ClassVar, Generic, Type

from syft_core import Client as SyftBoxClient
from syft_rds.client.rpc_clients.base import T, CreateT, UpdateT
from syft_rds.store import RDSStore
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class LocalStoreBase:
    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        self.config = config
        self.syftbox_client = syftbox_client

    def get_store(self, schema: Type[T]):
        self._schema_store = RDSStore(
            schema=schema,
            client=self.syftbox_client,
            datasite=self.config.host,
        )


class CRUDLocalStore(LocalStoreBase, Generic[T, CreateT, UpdateT]):
    SCHEMA: ClassVar[Type[T]]

    def __init__(self, config: "RDSClientConfig", syftbox_client: SyftBoxClient):
        super().__init__(config, syftbox_client)

    def create(self, item: CreateT) -> T:
        pass

    def get_one(self, request: GetOneRequest) -> T:
        pass

    def get_all(self, request: GetAllRequest) -> list[T]:
        pass

    def update(self, item: UpdateT) -> T:
        pass
