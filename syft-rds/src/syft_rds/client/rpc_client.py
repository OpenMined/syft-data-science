from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from syft_rds.client.connection import RPCConnection
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    JobUpdate,
    Runtime,
    RuntimeCreate,
    RuntimeUpdate,
    UserCode,
    UserCodeCreate,
    UserCodeUpdate,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


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


class RPCClient(RPCClientModule):
    def __init__(self, config: "RDSClientConfig", connection: RPCConnection):
        super().__init__(config, connection)
        self.jobs = JobRPCClient(self.config, self.connection)
        self.user_code = UserCodeRPCClient(self.config, self.connection)
        self.runtime = RuntimeRPCClient(self.config, self.connection)
        self.dataset = DatasetRPCClient(self.config, self.connection)


T = TypeVar("T", bound=ItemBase)
CreateT = TypeVar("CreateT", bound=ItemBaseCreate)
UpdateT = TypeVar("UpdateT", bound=ItemBaseUpdate)


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


class JobRPCClient(CRUDRPCClient[Job, JobCreate, JobUpdate]):
    MODULE_NAME = "job"


class UserCodeRPCClient(CRUDRPCClient[UserCode, UserCodeCreate, UserCodeUpdate]):
    MODULE_NAME = "user_code"


class RuntimeRPCClient(CRUDRPCClient[Runtime, RuntimeCreate, RuntimeUpdate]):
    MODULE_NAME = "runtime"


class DatasetRPCClient(CRUDRPCClient[Dataset, DatasetCreate, DatasetUpdate]):
    MODULE_NAME = "dataset"
