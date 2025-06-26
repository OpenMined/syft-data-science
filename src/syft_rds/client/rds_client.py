import subprocess
import threading
import time
from pathlib import Path
from typing import Optional, Type, TypeVar
from uuid import UUID

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_event import SyftEvents

from syft_rds.client.client_registry import GlobalClientRegistry
from syft_rds.client.connection import get_connection
from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import (
    RDSClientBase,
    RDSClientConfig,
    RDSClientModule,
)
from syft_rds.client.rds_clients.custom_function import CustomFunctionRDSClient
from syft_rds.client.rds_clients.dataset import DatasetRDSClient
from syft_rds.client.rds_clients.job import JobRDSClient
from syft_rds.client.rds_clients.runtime import RuntimeRDSClient
from syft_rds.client.rds_clients.user_code import UserCodeRDSClient
from syft_rds.client.rpc import RPCClient
from syft_rds.client.utils import PathLike, copy_dir_contents, deprecation_warning
from syft_rds.models import CustomFunction, Dataset, Job, JobStatus, Runtime, UserCode
from syft_rds.models.base import ItemBase
from syft_rds.syft_runtime.main import (
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
    TextUI,
    get_runner_cls,
)
from syft_rds.utils.constants import JOB_STATUS_POLLING_INTERVAL

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
        self,
        config: RDSClientConfig,
        rpc_client: RPCClient,
        local_store: LocalStore,
    ) -> None:
        super().__init__(config, rpc_client, local_store)
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
        self.runtime = RuntimeRDSClient(
            self.config, self.rpc, self.local_store, parent=self
        )

        # GlobalClientRegistry is used to register this client, to enable referencing the client from returned objects
        # e.g. Job._client references the RDSClient instance that created it.
        GlobalClientRegistry.register_client(self)

        self._type_map: dict[Type[T], RDSClientModule[T]] = {
            Job: self.job,
            Dataset: self.dataset,
            Runtime: self.runtime,
            UserCode: self.user_code,
            CustomFunction: self.custom_function,
        }

        self.start()

    def __del__(self) -> None:
        self.close()

    def start(self) -> None:
        self._start_job_polling()

    def close(self) -> None:
        if self._polling_stop_event.is_set():
            return
        logger.debug("Stopping job polling thread.")
        self._polling_stop_event.set()
        self._polling_thread.join(timeout=2)

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

    # TODO move all logic under here to a separate job handler module

    def run_private(
        self,
        job: Job,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
        blocking: bool = True,
    ) -> Job:
        if job.status == JobStatus.rejected:
            raise ValueError(
                "Cannot run rejected job. "
                "If you want to override this, set `job.status` to something else."
            )
        logger.debug(f"Running job '{job.name}' on private data")
        job_config: JobConfig = self._get_config_for_job(job, blocking=blocking)
        result = self._run(
            job,
            job_config,
            display_type,
            show_stdout,
            show_stderr,
        )

        if isinstance(result, tuple):  # result from a blocking job
            return_code, error_message = result
            job_update = job.get_update_for_return_code(
                return_code=return_code, error_message=error_message
            )
            return self.job.update_job_status(job_update, job)
        else:  # non-blocking job
            return self._register_nonblocking_job(result, job)

    def run_mock(
        self,
        job: Job,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
        blocking: bool = True,
    ) -> Job:
        logger.debug(f"Running job '{job.name}' on mock data")
        job_config: JobConfig = self._get_config_for_job(job, blocking=blocking)
        job_config.data_path = self.dataset.get(name=job.dataset_name).get_mock_path()
        result = self._run(
            job,
            job_config,
            display_type,
            show_stdout,
            show_stderr,
        )
        logger.info(f"Result from running job '{job.name}' on mock data: {result}")
        return job

    def _get_config_for_job(self, job: Job, blocking: bool = True) -> JobConfig:
        user_code = self.user_code.get(job.user_code_id)
        dataset = self.dataset.get(name=job.dataset_name)
        runtime = self.runtime.get(job.runtime_id)
        runner_config = self.config.runner_config
        job_config = JobConfig(
            data_path=dataset.get_private_path(),
            function_folder=user_code.local_dir,
            runtime=runtime,
            args=[user_code.entrypoint],
            job_folder=runner_config.job_output_folder / job.uid.hex,
            timeout=runner_config.timeout,
            blocking=blocking,
        )
        return job_config

    def _prepare_job(self, job: Job, config: JobConfig) -> None:
        if job.custom_function_id is not None:
            self._prepare_custom_function(
                code_dir=job.user_code.local_dir,
                custom_function_id=job.custom_function_id,
            )

    def _prepare_custom_function(
        self,
        code_dir: Path,
        custom_function_id: UUID,
    ) -> None:
        custom_function = self.custom_function.get(uid=custom_function_id)

        try:
            copy_dir_contents(
                src=custom_function.local_dir,
                dst=code_dir,
                exists_ok=False,
            )
        except FileExistsError as e:
            raise FileExistsError(
                f"Cannot copy custom function files to {code_dir}: {e}"
            ) from e

    def _get_display_handler(
        self, display_type: str, show_stdout: bool, show_stderr: bool
    ):
        """Returns the appropriate display handler based on the display type."""
        if display_type == "rich":
            return RichConsoleUI(
                show_stdout=show_stdout,
                show_stderr=show_stderr,
            )
        elif display_type == "text":
            return TextUI(
                show_stdout=show_stdout,
                show_stderr=show_stderr,
            )
        else:
            raise ValueError(f"Unknown display type: {display_type}")

    def _run(
        self,
        job: Job,
        job_config: JobConfig,
        display_type: str = "text",
        show_stdout: bool = True,
        show_stderr: bool = True,
    ) -> int | subprocess.Popen:
        display_handler = self._get_display_handler(
            display_type, show_stdout, show_stderr
        )
        runner_cls = get_runner_cls(job_config)
        runner = runner_cls(
            handlers=[FileOutputHandler(), display_handler],
            update_job_status_callback=self.job.update_job_status,
        )

        self._prepare_job(job, job_config)
        return runner.run(job, job_config)

    def _start_job_polling(self) -> None:
        """Starts the job polling mechanism."""
        logger.debug("Starting thread to poll jobs.")
        self._non_blocking_jobs: dict[UUID, tuple[Job, subprocess.Popen]] = {}
        self._jobs_lock = threading.Lock()
        self._polling_stop_event = threading.Event()
        self._polling_thread = threading.Thread(
            target=self._poll_update_nonblocking_jobs
        )
        self._polling_thread.daemon = True
        self._polling_thread.start()

    def _register_nonblocking_job(self, result: subprocess.Popen, job: Job) -> Job:
        with self._jobs_lock:
            self._non_blocking_jobs[job.uid] = (job, result)
        logger.debug(f"Non-blocking job '{job.name}' started with PID {result.pid}")
        return job

    def _poll_update_nonblocking_jobs(self) -> None:
        """
        Polls for non-blocking jobs and updates the job status accordingly.
        If a job is finished, it is removed from the list of non-blocking jobs.
        """
        while not self._polling_stop_event.is_set():
            with self._jobs_lock:
                finished_jobs = []
                for job_uid, (job, process) in self._non_blocking_jobs.items():
                    if process.poll() is not None:
                        finished_jobs.append(job_uid)
                        try:
                            return_code = process.returncode
                            stderr = process.stderr.read() if process.stderr else None

                            # TODO: remove this once we have a better way to handle errors
                            if return_code == 0 and stderr and "| ERROR" in stderr:
                                logger.debug(
                                    "Error detected in logs, even with return code 0."
                                )
                                return_code = 1

                            job_update = job.get_update_for_return_code(
                                return_code=return_code, error_message=stderr
                            )
                            self.job.update_job_status(job_update, job)
                            logger.debug(
                                f"Non-blocking job '{job.name}' (PID: {process.pid}) "
                                f"finished with code {return_code}."
                            )
                        except Exception as e:
                            logger.error(
                                f"Error updating status for job {job.name}: {e}"
                            )

                for job_uid in finished_jobs:
                    del self._non_blocking_jobs[job_uid]

            time.sleep(JOB_STATUS_POLLING_INTERVAL)
