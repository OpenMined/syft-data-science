from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pydantic import BaseModel
from syft_rpc import SyftResponse

from syft_rds.client.connection import BlockingRPCConnection
from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    ItemList,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


T = TypeVar("T", bound=BaseSchema)
CreateT = TypeVar("CreateT", bound=BaseSchemaCreate)
UpdateT = TypeVar("UpdateT", bound=BaseSchemaUpdate)


class RPCClientModule:
    def __init__(self, config: "RDSClientConfig", connection: BlockingRPCConnection):
        self.config = config
        self.connection = connection

        self.prefix = f"syft://{self.config.host}/api_data/{self.config.app_name}/rpc"

    def _send(self, path: str, body: BaseModel) -> SyftResponse:
        return self.connection.send(
            f"{self.prefix}/{path}",
            body,
            expiry=self.config.rpc_expiry,
            cache=False,
        )


class CRUDRPCClient(RPCClientModule, Generic[T, CreateT, UpdateT]):
    MODULE_NAME: ClassVar[str]
    MODEL_TYPE: ClassVar[type[T]]

    def create(self, item: CreateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/create", item)
        response.raise_for_status()

        return response.model(self.MODEL_TYPE)

    def get_one(self, request: GetOneRequest) -> T:
        response = self._send(f"{self.MODULE_NAME}/get_one", request)
        response.raise_for_status()

        return response.model(self.MODEL_TYPE)

    def get_all(self, request: GetAllRequest) -> list[T]:
        # TODO make ItemList[T] response model
        response = self._send(f"{self.MODULE_NAME}/get_all", request)
        response.raise_for_status()

        item_list = response.model(ItemList[self.MODEL_TYPE])
        return item_list.items

    def update(self, item: UpdateT) -> T:
        response = self._send(f"{self.MODULE_NAME}/update", item)
        response.raise_for_status()

        return response.model(self.MODEL_TYPE)
