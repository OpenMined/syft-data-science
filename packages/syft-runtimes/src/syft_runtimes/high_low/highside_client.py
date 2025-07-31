import shutil
from typing import Optional, Dict
from pathlib import Path

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_core.types import PathLike, to_path

from syft_runtimes.consts import RUNTIME_CONFIG_FILE
from syft_runtimes.high_low.consts import DEFAULT_HIGH_SIDE_DATA_DIR
from syft_runtimes.high_low.rsync import (
    RsyncConfig,
    RsyncEntry,
    SyncDirection,
    SyncResult,
    create_connection_from_config,
    LocalConnection,
    parse_rsync_command_for_display,
)
from syft_runtimes import HighLowRuntime

from syft_runtimes.high_low.rsync import (
    get_initial_sync_entries,
    get_sync_commands,
    execute_sync_commands,
)
from syft_runtimes.models import HighLowRuntimeConfig


class HighSideClient(SyftBoxClient):
    """Specialized SyftBox client for high-side operations in air-gapped environments."""

    def __init__(
        self,
        email: str,
        highside_identifier: str,
        syftbox_dir: PathLike,
        lowside_syftbox_dir: Optional[PathLike] = None,
        connection_config: Optional[Dict] = None,
        force_overwrite: bool = False,
    ):
        # Initialize data directory first
        syftbox_dir = HighSideClient._init_highside_syftbox_dir(
            email, syftbox_dir, force_overwrite
        )

        super().__init__(conf=self._create_highside_config(email, syftbox_dir))
        self.highside_identifier = highside_identifier

        # Store connection details
        self.connection = None
        self.lowside_syftbox_dir = None

        # Create datasite directory
        self.datasite_path.mkdir(parents=True, exist_ok=True)

        # Initialize the runtime and store it as an instance attribute
        self.runtime = HighLowRuntime(
            client=self,
            highside_identifier=highside_identifier,
        )
        self.runtime.init_runtime_dir()

        # Setup lowside connection if parameters provided
        if connection_config is not None or lowside_syftbox_dir is not None:
            self._setup_lowside_connection(
                highlow_identifier=highside_identifier,
                lowside_syftbox_dir=lowside_syftbox_dir,
                connection_config=connection_config,
            )

    @property
    def runtime_dir(self) -> Path:
        """Get the runtime directory path."""
        return self.runtime.runtime_dir

    @classmethod
    def initialize(
        cls,
        email: str,
        highside_identifier: str,
        syftbox_dir: PathLike,
        lowside_syftbox_dir: Optional[PathLike] = None,
        connection_config: Optional[Dict] = None,
        force_overwrite: bool = False,
    ) -> "HighSideClient":
        """Initialize a high datasite and optionally connect to lowside."""
        return cls(
            email=email,
            highside_identifier=highside_identifier,
            syftbox_dir=syftbox_dir,
            lowside_syftbox_dir=lowside_syftbox_dir,
            connection_config=connection_config,
            force_overwrite=force_overwrite,
        )

    def sync_dataset(
        self,
        dataset_name: str,
        verbose: bool = True,
    ) -> SyncResult:
        """Sync the public part of a specific dataset from high-side to low-side.

        Args:
            dataset_name: Name of the dataset to sync
            verbose: Whether to print sync progress information.
        """
        # Get dataset directories on high and low sides
        high_mock_dataset_dir = (
            self.my_datasite / "public" / "syft_datasets" / dataset_name
        )
        low_mock_dataset_dir = self._get_lowside_dataset_path(dataset_name)

        # Validate that the dataset exists on high-side
        if not high_mock_dataset_dir.exists():
            raise FileNotFoundError(
                f"Dataset '{dataset_name}' not found on high-side at {high_mock_dataset_dir}"
            )

        # Sync the mock part of the dataset to the low side
        sync_result = self._sync_with_rsync(
            local_dir=high_mock_dataset_dir,
            remote_dir=low_mock_dataset_dir,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            operation_description=f"mock part of dataset '{dataset_name}' from high-side to low-side",
            verbose=verbose,
        )

        # Add the dataset name to runtime's config.yaml
        runtime_config_path = self.runtime_dir / RUNTIME_CONFIG_FILE
        runtime_config = HighLowRuntimeConfig.from_yaml(runtime_config_path)
        if runtime_config.add_dataset(dataset_name):
            if verbose:
                logger.info(
                    f"Added dataset '{dataset_name}' to runtime config at {runtime_config_path}"
                )
        else:
            if verbose:
                logger.debug(
                    f"Dataset '{dataset_name}' already exists in runtime config at {runtime_config_path}"
                )

        # Sync the config.yaml to low side
        config_sync_result = self._sync_with_rsync(
            local_dir=runtime_config_path,
            remote_dir=self._get_lowside_runtime_dir() / RUNTIME_CONFIG_FILE,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            operation_description="config.yaml to low side",
            verbose=verbose,
            ignore_existing=False,
        )
        if verbose:
            logger.debug(
                f"Runtime's config.yaml synced to low side with result: {config_sync_result}"
            )

        return sync_result

    def sync_pending_jobs(
        self, ignore_existing: bool = True, verbose: bool = True
    ) -> SyncResult:
        """Sync jobs from low side to high side.

        Args:
            ignore_existing: If True, skip files that already exist at destination.
                            Default to True since jobs are mostly immutable once created.
            verbose: Whether to print sync progress information.
        """
        return self._sync_with_rsync(
            local_dir=self.runtime_dir / "jobs",
            remote_dir=self._get_lowside_runtime_dir() / "jobs",
            direction=SyncDirection.REMOTE_TO_LOCAL,
            ignore_existing=ignore_existing,
            operation_description="jobs from low-side to high-side",
            verbose=verbose,
        )

    def sync_done_jobs(
        self, ignore_existing: bool = False, verbose: bool = True
    ) -> SyncResult:
        """Sync completed job results to low side.

        Args:
            ignore_existing: If True, skip files that already exist at destination.
                        Default to False to ensure latest results are always synced, since
                        DO may choose to re-run the same job multiple times.
            verbose: Whether to print sync progress information.
        """
        return self._sync_with_rsync(
            local_dir=self.runtime_dir / "done",
            remote_dir=self._get_lowside_runtime_dir() / "done",
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=ignore_existing,
            operation_description="done jobs from high-side to low-side",
            verbose=verbose,
        )

    def run_private(self, job_id: str) -> dict:
        """Run a private job on high-side.

        Args:
            job_id: ID of the job to run
        """
        pass

    @staticmethod
    def _init_highside_syftbox_dir(
        email: str, syftbox_dir: PathLike | None, force_overwrite: bool = False
    ) -> PathLike:
        """Initialize the highside data directory."""
        if syftbox_dir:
            syftbox_dir = to_path(syftbox_dir)
        else:
            syftbox_dir = to_path(DEFAULT_HIGH_SIDE_DATA_DIR / email)

        if syftbox_dir.exists() and not force_overwrite:
            raise FileExistsError(
                f"Directory {syftbox_dir} already exists. Use force_overwrite=True to reset."
            )

        if force_overwrite and syftbox_dir.exists():
            logger.debug(f"Overwriting existing directory: {syftbox_dir}")
            shutil.rmtree(syftbox_dir)

        logger.debug(f"Creating high side SyftBox directory: {syftbox_dir}")
        syftbox_dir.mkdir(parents=True, exist_ok=True)

        return syftbox_dir

    def _create_highside_config(
        self, email: str, data_dir: PathLike
    ) -> SyftClientConfig:
        """Create SyftBox configuration for high-side."""
        config_path = to_path(data_dir) / "config.json"
        syftbox_data_dir = to_path(data_dir) / "SyftBox"

        logger.debug(f"Creating SyftBox configuration at: {config_path}")
        syft_config = SyftClientConfig(
            email=email,
            client_url="http://testserver:5000",
            path=config_path,
            data_dir=syftbox_data_dir,
        )
        syft_config.save()
        return syft_config

    def _setup_lowside_connection(
        self,
        highlow_identifier: str,
        lowside_syftbox_dir: Optional[PathLike] = None,
        connection_config: Optional[Dict] = None,
    ) -> None:
        """Setup connection to lowside and perform initial sync."""
        logger.debug(
            f"Setting up lowside connection with identifier: {highlow_identifier}"
        )

        # Create connection object
        self.connection = create_connection_from_config(
            lowside_data_dir=lowside_syftbox_dir, ssh_config=connection_config
        )

        # Store remote syftbox directory
        if isinstance(self.connection, LocalConnection):
            self.lowside_syftbox_dir = Path(self.connection.lowside_syftbox_dir)
        else:  # SSHConnection
            self.lowside_syftbox_dir = (
                Path(lowside_syftbox_dir)
                if lowside_syftbox_dir
                else Path("/home") / self.connection.user / "SyftBox"
            )

        # Create sync configuration
        sync_config = RsyncConfig.create_from_user_input(
            high_side_name=highlow_identifier,
            high_syftbox_dir=self.workspace.data_dir,
            syftbox_client_email=self.email,
            lowside_syftbox_dir=lowside_syftbox_dir,
            ssh_config=connection_config,
        )

        # Initial sync entries to create low side runtime structure
        default_entries = get_initial_sync_entries(sync_config)
        sync_config.entries.extend(default_entries)

        commands: list[str] = get_sync_commands(rsync_config=sync_config)
        sync_result = execute_sync_commands(commands, verbose=True)

        logger.debug(f"Initial sync result: {sync_result}")
        logger.info(
            f"Connected to low side with high-low identifier: {highlow_identifier}"
        )

    def _sync_with_rsync(
        self,
        local_dir: Path,
        remote_dir: Path,
        direction: SyncDirection,
        operation_description: str,
        verbose: bool = True,
        ignore_existing: bool = True,
    ) -> SyncResult:
        """Helper method to handle common rsync operations.

        Args:
            local_dir: Local directory path
            remote_dir: Remote directory path
            direction: Sync direction (LOCAL_TO_REMOTE or REMOTE_TO_LOCAL)
            operation_description: Description of the operation for logging
            verbose: Whether to print verbose output
            ignore_existing: If True, skip existing files. If False, update files that differ.

        Returns:
            SyncResult: Result of the sync operation
        """
        # Create the rsync entry
        rsync_entry = RsyncEntry(
            local_dir=local_dir,
            remote_dir=remote_dir,
            direction=direction,
            ignore_existing=ignore_existing,
        )

        # Generate and execute the rsync command
        command = rsync_entry.to_command(self.connection)

        if verbose:
            logger.debug(f"Sync command: {command}")
            logger.info(f"Syncing {operation_description}")

            # Log source and destination based on direction
            if direction == SyncDirection.LOCAL_TO_REMOTE:
                logger.info(f"Source: {local_dir}")
                logger.info(f"Destination: {remote_dir}")
            else:  # REMOTE_TO_LOCAL
                logger.info(f"Source: {remote_dir}")
                logger.info(f"Destination: {local_dir}")

            logger.debug(parse_rsync_command_for_display(command))

        sync_result = execute_sync_commands([command], verbose=verbose)

        if verbose:
            logger.debug(f"Synced {operation_description} with result: {sync_result}")

        return sync_result

    def _get_lowside_dataset_path(self, dataset_name: str) -> Path:
        """Calculate the path for a dataset on the lowside."""
        if not self.lowside_syftbox_dir:
            raise ValueError(
                "Lowside connection not configured. Call _setup_lowside_connection first."
            )

        if isinstance(self.connection, LocalConnection):
            # For local connections, use standard datasite structure
            return (
                self.lowside_syftbox_dir
                / "datasites"
                / self.email
                / "public"
                / "syft_datasets"
                / dataset_name
            )
        else:
            # For SSH connections, use remote datasite structure
            return (
                self.lowside_syftbox_dir
                / "datasites"
                / self.email
                / "public"
                / "syft_datasets"
                / dataset_name
            )

    def _get_lowside_runtime_dir(self) -> Path:
        """Calculate the runtime directory path on the lowside."""
        if not self.lowside_syftbox_dir:
            raise ValueError(
                "Lowside connection not configured. Call _setup_lowside_connection first."
            )

        return (
            self.lowside_syftbox_dir
            / "private"
            / self.email
            / "syft_runtimes"
            / self.highside_identifier
        )


def initialize_high_datasite(
    email: str,
    highlow_identifier: str,
    force_overwrite: bool = False,
    connection_config: Optional[Dict] = None,
    lowside_syftbox_dir: Optional[PathLike] = None,
    syftbox_dir: Optional[PathLike] = None,
) -> HighSideClient:
    """Initialize a high datasite with SyftBox configuration and connect to lowside.

    Args:
        email: Email address for the datasite
        highlow_identifier: Unique identifier for the high-low runtime connection
        force_overwrite: Whether to overwrite existing directories
        connection_config: SSH connection configuration (for remote lowside)
        lowside_syftbox_dir: Path to SyftBox directory on lowside
        syftbox_dir: Path for highside SyftBox directory (optional)

    Returns:
        HighSideClient: Initialized high-side client with lowside connection
    """
    return HighSideClient.initialize(
        email=email,
        highside_identifier=highlow_identifier,
        syftbox_dir=syftbox_dir,
        lowside_syftbox_dir=lowside_syftbox_dir,
        connection_config=connection_config,
        force_overwrite=force_overwrite,
    )
