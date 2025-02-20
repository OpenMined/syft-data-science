from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel
from syft_rds.client.connection import RPCConnection
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    Job,
    JobCreate,
    Runtime,
    RuntimeCreate,
    UserCode,
    UserCodeCreate,
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


class JobRPCClient(RPCClientModule):
    def create(self, item: JobCreate) -> Job:
        return self._send("job/create", item)

    def get(self, uid: UUID) -> Job:
        pass

    def get_one(self, name: str | None = None) -> Job:
        pass

    def get_all(self, name: str | None = None) -> list[Job]:
        pass


class UserCodeRPCClient(RPCClientModule):
    def create(self, item: UserCodeCreate) -> UserCode:
        return self._send("user_code/create", item)

    def get(self, uid: UUID) -> UserCode:
        pass


class RuntimeRPCClient(RPCClientModule):
    def create(self, item: RuntimeCreate) -> Runtime:
        pass

    def get(self, uid: UUID) -> Runtime:
        pass

    def get_one(self, name: str | None = None) -> Runtime:
        pass

    def get_all(self, name: str | None = None) -> list[Runtime]:
        pass


class DatasetRPCClient(RPCClientModule):
    def create(self, item: DatasetCreate) -> Dataset:
        pass

    def get(self, uid: UUID) -> Dataset:
        pass

    def get_one(self, name: str | None = None) -> Dataset:
        pass

    def get_all(self, name: str | None = None) -> list[Dataset]:
        pass
