from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, Optional, Type
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from syft_core import Client as SyftBoxClient

from syft_rds.client.local_store import LocalStore
from syft_rds.client.rpc import RPCClient, T
from syft_rds.models import GetAllRequest, GetOneRequest, Job, Runtime

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class ClientRunnerConfig(BaseModel):
    runtime: Runtime | None = None
    timeout: int = 60
    job_output_folder: Path = Field(
        default_factory=lambda: Path(".server/syft-rds-jobs")
    )


class RDSClientConfig(BaseModel):
    uid: UUID = Field(
        default_factory=uuid4
    )  # used to register the client & reference it from other objects
    host: str
    app_name: str = "RDS"
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
    ITEM_TYPE: ClassVar[Type[T]]

    def __init__(
        self,
        config: RDSClientConfig,
        rpc_client: RPCClient,
        local_store: LocalStore,
        parent: "Optional[RDSClient]" = None,
    ) -> None:
        super().__init__(config, rpc_client, local_store)
        self.parent = parent

    @property
    def rds(self) -> "RDSClient":
        """
        Returns the parent RDSClient, raises an error if not set.
        Used for accessing other client modules from this module, e.g. JobRDSClient().rds.dataset.get_all() -> DatasetRDSClient
        """
        if self.parent is None:
            raise ValueError("Parent client not set")
        return self.parent

    def get_all(
        self,
        order_by: str = "created_at",
        sort_order: str = "desc",
        limit: Optional[int] = None,
        offset: int = 0,
        mode: Literal["local", "rpc"] = "local",
        **filters: Any,
    ) -> list[Job]:
        req = GetAllRequest(
            order_by=order_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            filters=filters,
        )

        if mode == "local":
            return self.local_store.for_type(self.ITEM_TYPE).get_all(req)
        elif mode == "rpc":
            return self.rpc.for_type(self.ITEM_TYPE).get_all(req)
        else:
            raise ValueError(f"Invalid mode {mode}")

    def get(
        self,
        uid: Optional[UUID] = None,
        mode: Literal["local", "rpc"] = "local",
        **filters: Any,
    ) -> T:
        req = GetOneRequest(uid=uid, filters=filters)
        if mode == "local":
            return self.local_store.for_type(self.ITEM_TYPE).get_one(req)
        elif mode == "rpc":
            return self.rpc.for_type(self.ITEM_TYPE).get_one(req)
        else:
            raise ValueError(f"Invalid mode {mode}")
