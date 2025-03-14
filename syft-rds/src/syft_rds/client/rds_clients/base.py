from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, Type
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from syft_core import Client as SyftBoxClient

from syft_rds.client.local_store import LocalStore
from syft_runtime.main import CodeRuntime
from syft_rds.client.rpc import RPCClient, T
from syft_rds.models.models import GetAllRequest, GetOneRequest, Job

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class ClientRunnerConfig(BaseModel):
    runtime: CodeRuntime = CodeRuntime(cmd=["python"])
    timeout: int = 60
    use_docker: bool = False
    job_output_folder: Path = Field(default_factory=lambda: Path("/tmp/syft-rds-jobs"))


class RDSClientConfig(BaseModel):
    host: str
    app_name: str = "RDS"
    client_id: UUID = Field(default_factory=uuid4)

    rpc_expiry: str = "5m"
    runner_config: ClientRunnerConfig = Field(default_factory=ClientRunnerConfig)


class RDSClientBase:
    def __init__(
        self, config: RDSClientConfig, rpc_client: RPCClient, local_store: LocalStore
    ) -> None:
        self.config = config
        self.rpc = rpc_client
        self.local_store = local_store

    @property
    def host(self) -> str:
        return self.config.host

    @property
    def _syftbox_client(self) -> SyftBoxClient:
        return self.rpc.connection.sender_client

    @property
    def email(self) -> str:
        return self._syftbox_client.email

    @property
    def is_admin(self) -> bool:
        return self.host == self.email


class RDSClientModule(RDSClientBase, Generic[T]):
    SCHEMA: ClassVar[Type[T]]

    def __init__(
        self,
        config: RDSClientConfig,
        rpc_client: RPCClient,
        local_store: LocalStore,
        parent: "Optional[RDSClient]" = None,
    ) -> None:
        """
        NOTE `parent` is used to access other client modules from the current module.
        for example: in the Job client, we can access the Dataset client using `self.rds.dataset`
        """
        super().__init__(config, rpc_client, local_store)
        self.parent = parent

    @property
    def rds(self) -> "RDSClient":
        if self.parent is None:
            raise ValueError("Parent client not set")
        return self.parent

    def get_all(
        self,
        order_by: str = "created_at",
        sort_order: str = "desc",
        limit: Optional[int] = None,
        offset: int = 0,
        **filters: Any,
    ) -> list[Job]:
        store = self.local_store.for_type(self.SCHEMA)
        return store.get_all(
            GetAllRequest(
                order_by=order_by,
                sort_order=sort_order,
                limit=limit,
                offset=offset,
                filters=filters,
            )
        )

    def get(self, uid: Optional[UUID] = None, **filters: Any) -> T:
        store = self.local_store.for_type(self.SCHEMA)
        return store.get_one(
            GetOneRequest(
                uid=uid,
                filters=filters,
            )
        )
