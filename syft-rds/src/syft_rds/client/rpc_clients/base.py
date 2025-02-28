from typing import TYPE_CHECKING

from pydantic import BaseModel
from typing import ClassVar, Generic, TypeVar

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.client.connection import RPCConnection
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


T = TypeVar("T", bound=BaseSchema)
CreateT = TypeVar("CreateT", bound=BaseSchemaCreate)
UpdateT = TypeVar("UpdateT", bound=BaseSchemaUpdate)


class RPCClientModule:
    def __init__(self, config: "RDSClientConfig", connection: RPCConnection):
        self.config = config
        self.connection = connection

        self.prefix = f"syft://{self.config.host}/api_data/{self.config.app_name}/rpc"

    def _send(self, path: str, body: BaseModel) -> dict:
        return self.connection.send(
            f"{self.prefix}/{path}",
            body,
            expiry=self.config.rpc_expiry,
            cache=self.config.rpc_cache,
        )


class CRUDRPCClient(RPCClientModule, Generic[T, CreateT, UpdateT]):
    MODULE_NAME: ClassVar[str]

    def create(self, item: CreateT) -> T:
        return self._send(f"{self.MODULE_NAME}/create", item)

    def get_one(self, request: GetOneRequest) -> T:
        return self._send(f"{self.MODULE_NAME}/get_one", request)

    def get_all(self, request: GetAllRequest) -> list[T]:
        return self._send(f"{self.MODULE_NAME}/get_all", request)

    def update(self, item: UpdateT) -> T:
        return self._send(f"{self.MODULE_NAME}/update", item)
