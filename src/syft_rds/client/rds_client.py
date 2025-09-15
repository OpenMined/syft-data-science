import atexit
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
from syft_rds.server.app import create_app
from syft_rds.syft_runtime.main import (
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
    TextUI,
    get_runner_cls,
)
from syft_rds.utils.constants import JOB_STATUS_POLLING_INTERVAL

T = TypeVar("T", bound=ItemBase)

# Global registry to track running servers
_RUNNING_RDS_SERVERS = {}


def _is_server_running(host: str) -> bool:
    """Check if syft-rds server is running for the given host."""
    try:
        # Create a minimal config and connection to test server health
        config = RDSClientConfig(host=host)
        syftbox_client = _resolve_syftbox_client()
        connection = get_connection(syftbox_client, None, mock=False)
        rpc_client = RPCClient(config, connection)

        # Try health check with short timeout
        rpc_client.health(expiry="3s")
        return True
    except Exception:
        return False


def _start_server_thread(syftbox_client: SyftBoxClient) -> dict:
    """Start syft-rds server in a background thread."""
    rds_app: SyftEvents = create_app(client=syftbox_client)

    def run_server():
        try:
            logger.info(f"Starting syft-rds server for {syftbox_client.email}")
            rds_app.run_forever()
        except Exception as e:
            logger.error(f"Server thread failed: {e}")

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    return {"thread": thread, "server": rds_app}


def _wait_for_server(host: str, timeout: int = 30) -> bool:
    """Wait for server to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if _is_server_running(host):
            return True
        time.sleep(0.5)
    return False


def _ensure_server_running(
    syftbox_client: SyftBoxClient, auto_start: bool = False
) -> bool:
    """Ensure syft-rds server is running, starting it if needed."""
    host = syftbox_client.email

    if _is_server_running(host):
        return True

    if not auto_start:
        return False

    # Check if we already have a server thread for this client
    if host in _RUNNING_RDS_SERVERS and _RUNNING_RDS_SERVERS[host]["thread"].is_alive():
        # Server thread exists, wait for it to be ready
        if _wait_for_server(host):
            return True

    logger.info(f"syft-rds server not running for {host}, starting automatically...")

    # Start new server thread
    server_info = _start_server_thread(syftbox_client)
    _RUNNING_RDS_SERVERS[host] = server_info

    # Register cleanup on exit
    atexit.register(_cleanup_servers)

    # Wait for server to be ready
    if _wait_for_server(host):
        logger.success(f"syft-rds server is ready for {host}")
        return True
    else:
        logger.error(f"syft-rds server failed to start for {host}")
        return False


def _stop_server(host: str) -> bool:
    """Stop the syft-rds server for a specific host.

    Args:
        host: Email of the host whose server to stop

    Returns:
        True if server was stopped, False if no server was running
    """
    if host not in _RUNNING_RDS_SERVERS:
        return False

    server_info = _RUNNING_RDS_SERVERS[host]
    thread = server_info["thread"]
    server = server_info["server"]

    if thread.is_alive():
        logger.info(f"Stopping syft-rds server for {host}")
        try:
            server.stop()
            thread.join(timeout=5)  # Wait up to 5 seconds for clean shutdown
            if thread.is_alive():
                logger.warning(f"Server thread for {host} did not stop gracefully")
        except Exception as e:
            logger.error(f"Error stopping server for {host}: {e}")

    del _RUNNING_RDS_SERVERS[host]
    logger.success(f"Stopped syft-rds server for {host}")
    return True


def _cleanup_servers() -> None:
    """Clean up server threads on exit."""
    for host, server_info in _RUNNING_RDS_SERVERS.items():
        thread = server_info["thread"]
        server = server_info["server"]

        if thread.is_alive():
            logger.debug(f"Cleaning up server thread for {host}")
            try:
                server.stop()
            except Exception as e:
                logger.debug(f"Error during server cleanup for {host}: {e}")

    _RUNNING_RDS_SERVERS.clear()


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
    start_rds_server: bool = False,
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
        start_rds_server (bool, optional): Whether to automatically start syft-rds server
            if it's not running (same as `uv run syft-rds server`). Defaults to False.
        **config_kwargs: Additional configuration options for the RDSClient.

    Returns:
        RDSClient: The configured RDS client instance.
    """
    config = RDSClientConfig(host=host, **config_kwargs)
    syftbox_client = _resolve_syftbox_client(syftbox_client, syftbox_client_config_path)

    # Auto-start server if not using mock and auto_start_server is enabled
    use_mock = mock_server is not None
    if not use_mock and start_rds_server:
        server_started = _ensure_server_running(syftbox_client, start_rds_server)
        if not server_started:
            logger.warning(
                f"Failed to start syft-rds server for {host}. "
                "You may need to start it manually by running `uv run syft-rds server` in a separate terminal."
            )

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

    def stop_server(self) -> bool:
        """Stop the syft-rds server for this client's host.

        Returns:
            True if server was stopped, False if no server was running for this host
        """
        return _stop_server(self.config.host)
