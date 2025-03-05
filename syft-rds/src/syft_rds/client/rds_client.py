from pathlib import Path
from typing import Optional

from syft_core import Client as SyftBoxClient
from syft_event import SyftEvents

from syft_rds.client.connection import get_connection
from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import RDSClientConfig, RDSClientModule
from syft_rds.client.rds_clients.dataset import DatasetRDSClient
from syft_rds.client.rds_clients.jobs import JobRDSClient
from syft_rds.client.rds_clients.runtime import RuntimeRDSClient
from syft_rds.client.rds_clients.user_code import UserCodeRDSClient
from syft_rds.client.rpc import RPCClient
from syft_rds.client.utils import PathLike
from syft_rds.models.models import Dataset, Job
from syft_runtime.main import DockerRunner, FileOutputHandler, JobConfig, RichConsoleUI


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

    Returns:
        RDSClient: The configured RDS client instance.
    """
    config = RDSClientConfig(host=host)
    syftbox_client = _resolve_syftbox_client(syftbox_client, syftbox_client_config_path)

    use_mock = mock_server is not None
    connection = get_connection(syftbox_client, mock_server, mock=use_mock)
    rpc_client = RPCClient(config, connection)
    local_store = LocalStore(config, syftbox_client)
    return RDSClient(config, rpc_client, local_store)


class RDSClient(RDSClientModule):
    def __init__(
        self, config: RDSClientConfig, rpc_client: RPCClient, local_store: LocalStore
    ) -> None:
        super().__init__(config, rpc_client, local_store)
        self.jobs = JobRDSClient(self.config, self.rpc, self.local_store)
        self.runtime = RuntimeRDSClient(self.config, self.rpc, self.local_store)
        self.dataset = DatasetRDSClient(self.config, self.rpc, self.local_store)
        self.user_code = UserCodeRDSClient(self.config, self.rpc, self.local_store)

    @property
    def datasets(self) -> list[Dataset]:
        """Returns all available datasets.

        Returns:
            list[Dataset]: A list of all datasets
        """
        return self.dataset.get_all()

    def run(self, job: Job) -> None:
        """Runs a job.

        Args:
            job (Job): The job to run
        """

        config = JobConfig(
            function_folder=job.user_code.path.parent,
            args=[job.user_code.path.name],
            data_path=self.dataset.get(job.dataset_name).get_mock_path(),
            runtime=job.runtime,
            job_folder=str(job.name),
            timeout=1,
            use_docker=False,
        )

        runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])
        return_code = runner.run(config)
        return return_code
