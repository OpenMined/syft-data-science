from typing import TYPE_CHECKING
from uuid import UUID

from syft_rds.models.models import (
    Job,
    JobCreate,
    Runtime,
    RuntimeCreate,
    UserCode,
    UserCodeCreate,
)

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClientConfig


class RPCClient:
    def __init__(self, host: str):
        self.host = host
        self.jobs = JobRPCClient(self.host)
        self.user_code = UserCodeRPCClient(self.host)


class RPCClientModule:
    def __init__(self, config: RDSClientConfig):
        self.config = config


class JobRPCClient(RPCClientModule):
    def create(self, item: JobCreate) -> Job:
        pass

    def get(self, uid: UUID) -> Job:
        pass

    def get_one(self, name: str | None = None) -> Job:
        pass

    def get_all(self, name: str | None = None) -> list[Job]:
        pass


class UserCodeRPCClient(RPCClientModule):
    def create(self, item: UserCodeCreate) -> UserCode:
        pass

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
