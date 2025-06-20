from pathlib import Path
from typing import Optional, Type, TypeVar
from uuid import UUID

from syft_core import Client as SyftBoxClient
from syft_event import SyftEvents

from syft_rds.client.client_registry import GlobalClientRegistry
from syft_rds.client.connection import get_connection
from syft_rds.client.job_runner import JobRunner
from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import (
    RDSClientBase,
    RDSClientConfig,
    RDSClientModule,
)
from syft_rds.client.rds_clients.custom_function import CustomFunctionRDSClient
from syft_rds.client.rds_clients.dataset import DatasetRDSClient
from syft_rds.client.rds_clients.job import JobRDSClient
from syft_rds.client.rds_clients.user_code import UserCodeRDSClient
from syft_rds.client.rpc import RPCClient
from syft_rds.client.utils import PathLike, deprecation_warning
from syft_rds.models import CustomFunction, Dataset, Job, UserCode
from syft_rds.models.base import ItemBase
from syft_rds.syft_runtime.main import (
    JobConfig,
)

T = TypeVar("T", bound=ItemBase)


def _resolve_syftbox_client(
    syftbox_client: Optional[SyftBoxClient] = None,
    config_path: Optional[PathLike] = None,
) -> SyftBoxClient:
    """
    Resolve a SyftBox client from either a provided instance or config path.

    Args:
        syftbox_client (SyftBoxClient, optional): Pre-configured client instance
        config_path (Union[str, Path], optional): Path to client config file

    Returns:
        SyftBoxClient: The SyftBox client instance

    Raises:
        ValueError: If both syftbox_client and config_path are provided
    """
    if (
        syftbox_client
        and config_path
        and syftbox_client.config_path.resolve() != Path(config_path).resolve()
    ):
        raise ValueError("Cannot provide both syftbox_client and config_path.")

    if syftbox_client:
        return syftbox_client

    return SyftBoxClient.load(filepath=config_path)


def init_session(
    host: str,
    syftbox_client: Optional[SyftBoxClient] = None,
    mock_server: Optional[SyftEvents] = None,
    syftbox_client_config_path: Optional[PathLike] = None,
    **config_kwargs,
) -> "RDSClient":
    """
    Initialize a session with the RDSClient.

    Args:
        host (str): The email of the remote datasite
        syftbox_client (SyftBoxClient, optional): Pre-configured SyftBox client instance.
            Takes precedence over syftbox_client_config_path.
        mock_server (SyftEvents, optional): Server for testing. If provided, uses
            a mock in-process RPC connection.
        syftbox_client_config_path (PathLike, optional): Path to client config file.
            Only used if syftbox_client is not provided.
        **config_kwargs: Additional configuration options for the RDSClient.

    Returns:
        RDSClient: The configured RDS client instance.
    """
    config = RDSClientConfig(host=host, **config_kwargs)
    syftbox_client = _resolve_syftbox_client(syftbox_client, syftbox_client_config_path)

    use_mock = mock_server is not None
    connection = get_connection(syftbox_client, mock_server, mock=use_mock)
    rpc_client = RPCClient(config, connection)
    local_store = LocalStore(config, syftbox_client)
    return RDSClient(config, rpc_client, local_store)


class RDSClient(RDSClientBase):
    def __init__(
        self, config: RDSClientConfig, rpc_client: RPCClient, local_store: LocalStore
    ) -> None:
        super().__init__(config, rpc_client, local_store)
        self.job_runner = JobRunner(self)

        self.job = JobRDSClient(self.config, self.rpc, self.local_store, parent=self)
        self.dataset = DatasetRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )
        self.user_code = UserCodeRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )
        self.custom_function = CustomFunctionRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )

        # TODO implement and enable runtime client
        # self.runtime = RuntimeRDSClient(self.config, self.rpc, self.local_store)
        GlobalClientRegistry.register_client(self)

        self._type_map: dict[Type[T], RDSClientModule[T]] = {
            Job: self.job,
            Dataset: self.dataset,
            # Runtime: self.runtime,
            UserCode: self.user_code,
            CustomFunction: self.custom_function,
        }

    def for_type(self, type_: Type[T]) -> RDSClientModule[T]:
        if type_ not in self._type_map:
            raise ValueError(f"No client registered for type {type_}")
        return self._type_map[type_]

    @property
    def uid(self) -> UUID:
        return self.config.uid

    @property
    @deprecation_warning(reason="client.jobs has been renamed to client.job")
    def jobs(self) -> JobRDSClient:
        return self.job

    @property
    @deprecation_warning(reason="Use client.dataset.get_all() instead.")
    def datasets(self) -> list[Dataset]:
        """Returns all available datasets.

        Returns:
            list[Dataset]: A list of all datasets
        """
        return self.dataset.get_all()

    def run_private(
        self,
        job: Job,
        config: Optional[JobConfig] = None,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> Job:
        return self.job_runner.run_private(
            job=job,
            config=config,
            display_type=display_type,
            show_stdout=show_stdout,
            show_stderr=show_stderr,
        )

    def run_mock(
        self,
        job: Job,
        config: Optional[JobConfig] = None,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> Job:
        return self.job_runner.run_mock(
            job=job,
            config=config,
            display_type=display_type,
            show_stdout=show_stdout,
            show_stderr=show_stderr,
        )
