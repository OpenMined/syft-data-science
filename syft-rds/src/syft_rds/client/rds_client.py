from pathlib import Path
from typing import Optional, Type, TypeVar
from uuid import UUID

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_event import SyftEvents
from syft_rds.syft_runtime.main import (
    DockerRunner,
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
)

from syft_rds.client.client_registry import GlobalClientRegistry
from syft_rds.client.connection import get_connection
from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import (
    RDSClientBase,
    RDSClientConfig,
    RDSClientModule,
)
from syft_rds.client.rds_clients.dataset import DatasetRDSClient
from syft_rds.client.rds_clients.jobs import JobRDSClient
from syft_rds.client.rds_clients.user_code import UserCodeRDSClient
from syft_rds.client.rpc import RPCClient
from syft_rds.client.utils import PathLike
from syft_rds.models.base import ItemBase
from syft_rds.models.models import Dataset, Job, JobStatus, UserCode

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
        self.jobs = JobRDSClient(self.config, self.rpc, self.local_store, parent=self)
        self.dataset = DatasetRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )
        self.user_code = UserCodeRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )

        # TODO implement and enable runtime client
        # self.runtime = RuntimeRDSClient(self.config, self.rpc, self.local_store)
        GlobalClientRegistry.register_client(self)

        self._type_map = {
            Job: self.jobs,
            Dataset: self.dataset,
            # Runtime: self.runtime,
            UserCode: self.user_code,
        }

    def for_type(self, type_: Type[T]) -> RDSClientModule[T]:
        if type_ not in self._type_map:
            raise ValueError(f"No client registered for type {type_}")
        return self._type_map[type_]

    @property
    def uid(self) -> UUID:
        return self.config.uid

    @property
    def datasets(self) -> list[Dataset]:
        """Returns all available datasets.

        Returns:
            list[Dataset]: A list of all datasets
        """
        return self.dataset.get_all()

    def get_default_config_for_job(self, job: Job) -> JobConfig:
        user_code = self.user_code.get(job.user_code_id)
        dataset = self.dataset.get(name=job.dataset_name)
        runtime = dataset.runtime or self.config.runner_config.runtime
        runner_config = self.config.runner_config
        return JobConfig(
            function_folder=user_code.local_dir,
            args=[user_code.entrypoint],
            data_path=dataset.get_private_path(),
            runtime=runtime,
            job_folder=runner_config.job_output_folder / job.uid.hex,
            timeout=runner_config.timeout,
            use_docker=runner_config.use_docker,
        )

    def run_private(self, job: Job, config: Optional[JobConfig] = None) -> Job:
        if job.status == JobStatus.rejected:
            raise ValueError(
                "Cannot run rejected job, "
                "if you want to override this, "
                "set job.status to something else"
            )

        config = config or self.get_default_config_for_job(job)

        logger.warning("Running job without docker is not secure")
        return_code = self._run(config=config)
        job_update = job.get_update_for_return_code(return_code)
        new_job = self.rpc.jobs.update(job_update)
        return job.apply_update(new_job)

    def run_mock(self, job: Job, config: Optional[JobConfig] = None) -> Job:
        config = config or self.get_default_config_for_job(job)
        config.data_path = self.dataset.get(name=job.dataset_name).get_mock_path()
        self._run(config=config)
        return job

    def _run(self, config: JobConfig) -> int:
        """Runs a job.

        Args:
            job (Job): The job to run
        """

        runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])
        return_code = runner.run(config)
        return return_code
