from pathlib import Path
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from syft_core import Client as SyftBoxClient

from syft_rds.client.local_store import LocalStore
from syft_rds.client.rpc import RPCClient
from syft_runtime.main import CodeRuntime


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


class RDSClientModule:
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
