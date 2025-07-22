from typing import Optional, Dict
from pathlib import Path

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_core.types import PathLike, to_path

from syft_runtimes.high_low.rsync import (
    RsyncConfig,
    SSHConnection,
    create_connection_from_config,
    LocalConnection,
)
from syft_runtimes import HighLowRuntime
from syft_runtimes.high_low.lowside_client import (
    LowSideClient,
    LocalLowSideClient,
    SSHLowSideClient,
)
from syft_runtimes.high_low.setup import _init_highside_data_dir
from syft_runtimes.high_low.rsync import (
    _get_initial_sync_entries,
    _get_sync_commands,
    _execute_sync_commands,
)


class HighSideClient(SyftBoxClient):
    """Specialized SyftBox client for high-side operations in air-gapped environments."""

    def __init__(
        self,
        email: str,
        highside_identifier: str,
        data_dir: PathLike,
    ):
        super().__init__(conf=self._create_highside_config(email, data_dir))
        self.highside_identifier = highside_identifier

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

    @classmethod
    def initialize(
        cls,
        email: str,
        highside_identifier: str,
        data_dir: PathLike,
        force_overwrite: bool = False,
    ) -> "HighSideClient":
        """Initialize a high datasite."""
        data_dir = _init_highside_data_dir(email, data_dir, force_overwrite)

        # Create the client and its basic data dir
        client = cls(
            email=email, highside_identifier=highside_identifier, data_dir=data_dir
        )
        client.datasite_path.mkdir(parents=True, exist_ok=True)

        # Init the runtime
        highside_runtime = HighLowRuntime(
            client=client,
            highside_identifier=highside_identifier,
        )
        highside_runtime.init_runtime_dir()

        return client

    def lowside_connect(
        self,
        highlow_identifier: str,
        lowside_data_dir: Optional[PathLike] = None,
        ssh_config: Optional[Dict] = None,
        force_overwrite: bool = False,
    ) -> LowSideClient:
        """
        Connect to low side and create sync configuration.

        Args:
            highlow_identifier: Unique identifier for this high-low runtime connection
            lowside_data_dir: Path to local SyftBox directory (for local connections)
            ssh_config: Dict with SSH connection details (for remote connections)
                Required keys: 'host', 'user'
                Optional keys: 'port' (default: 22), 'ssh_key_path'
            force_overwrite: Whether to overwrite existing configuration

        Returns:
            SyftBoxClient: The low-side client

        Examples:
            # Local connection
            lowside_client = highside_client.lowside_connect(
                highlow_identifier="my-runtime",
                lowside_data_dir="/path/to/local/syftbox"
            )

            # SSH connection
            lowside_client = highside_client.lowside_connect(
                highlow_identifier="my-runtime",
                ssh_config={
                    "host": "lowside.example.com",
                    "user": "myuser",
                    "port": 2222,
                    "ssh_key_path": "/path/to/key"
                }
            )
        """
        logger.debug(f"Connecting to low side with identifier: {highlow_identifier}")

        # Validate inputs using our helper function
        connection = create_connection_from_config(lowside_data_dir, ssh_config)

        # Create lowside client based on connection type
        if isinstance(connection, LocalConnection):
            lowside_client = self._create_local_lowside_client(
                connection, force_overwrite
            )
        else:  # SSHConnection
            lowside_client = self._create_ssh_lowside_client(
                connection, lowside_data_dir
            )

        # Create sync configuration using the new user-friendly method
        sync_config = RsyncConfig.create_from_user_input(
            high_side_name=highlow_identifier,
            high_syftbox_dir=self.workspace.data_dir,
            syftbox_client_email=self.email,
            lowside_data_dir=lowside_data_dir,
            ssh_config=ssh_config,
        )

        # Add default sync entries (jobs and outputs)
        default_entries = _get_initial_sync_entries(sync_config)
        sync_config.entries.extend(default_entries)

        # Save rsync configuration
        sync_config.save(self)

        # Store the configuration and client for later use
        self._sync_config = sync_config
        self._lowside_client = lowside_client

        commands: list[str] = _get_sync_commands(rsync_config=sync_config)

        if not commands:
            logger.debug("No runtime sync commands to execute.")
            return

        _execute_sync_commands(commands, verbose=True)

        logger.info(
            f"Successfully connected to low side with identifier: {highlow_identifier}"
        )

        return lowside_client

    def _create_local_lowside_client(
        self, connection: LocalConnection, force_overwrite: bool = False
    ) -> LocalLowSideClient:
        """Create a LocalLowSideClient for local low-side connection."""
        lowside_data_dir = to_path(connection.lowside_syftbox_dir)

        # Create config and data directories
        config_path = lowside_data_dir / "config.json"

        if config_path.exists() and not force_overwrite:
            logger.debug(f"Loading existing low-side client from {config_path}")
            config = SyftClientConfig.load(config_path)
        else:
            logger.debug(f"Creating new low-side client at {config_path}")
            config = SyftClientConfig(
                email=self.email,  # Use same email as high-side
                client_url="http://testserver:5000",  # Default for local development
                path=config_path,
                data_dir=lowside_data_dir,
            )
            config.save()

        syftbox_client = SyftBoxClient(conf=config)
        syftbox_client.datasite_path.mkdir(parents=True, exist_ok=True)

        return LocalLowSideClient(syftbox_client)

    def _create_ssh_lowside_client(
        self,
        connection: SSHConnection,
        remote_syftbox_dir: Optional[PathLike] = None,
    ) -> SSHLowSideClient:
        """Create an SSHLowSideClient for SSH low-side connection."""
        ssh_config = {
            "host": connection.host,
            "user": connection.user,
            "port": connection.port,
            "ssh_key_path": connection.ssh_key_path,
        }

        # For SSH connections, we need to determine the remote SyftBox directory
        # This could be passed as a parameter or discovered via SSH
        # For now, we'll use a sensible default
        remote_syftbox_dir = (
            remote_syftbox_dir or Path("/home") / connection.user / "SyftBox"
        )

        return SSHLowSideClient(
            email=self.email,
            ssh_config=ssh_config,
            remote_syftbox_dir=remote_syftbox_dir,
        )

    # def sync_dataset(self, dataset_name: str, verbose: bool = True) -> None:
    #     """Sync dataset mock data to low side."""
    #     if not self._sync_config or not self._lowside_client:
    #         raise RuntimeError(
    #             "Low-side connection not established. Call lowside_connect() first."
    #         )

    #     sync_dataset(
    #         dataset_name=dataset_name,
    #         highside_client=self,
    #         lowside_client=self._lowside_client,
    #         verbose=verbose,
    #     )

    # def sync_pending_jobs(self, verbose: bool = True) -> None:
    #     """Sync jobs from low side to high side."""
    #     if not self._sync_config:
    #         raise RuntimeError(
    #             "Sync configuration not found. Call lowside_connect() first."
    #         )

    #     sync(
    #         direction=SyncDirection.REMOTE_TO_LOCAL,
    #         syftbox_client=self,
    #         rsync_config=self._sync_config,
    #         verbose=verbose,
    #     )

    # def sync_done_jobs(self, verbose: bool = True) -> None:
    #     """Sync completed job results to low side."""
    #     if not self._sync_config:
    #         raise RuntimeError(
    #             "Sync configuration not found. Call lowside_connect() first."
    #         )

    #     sync(
    #         direction=SyncDirection.LOCAL_TO_REMOTE,
    #         syftbox_client=self,
    #         rsync_config=self._sync_config,
    #         verbose=verbose,
    #     )

    # @property
    # def sync_config(self) -> Optional[RsyncConfig]:
    #     """Get the current sync configuration."""
    #     return self._sync_config


def initialize_high_datasite(
    email: str,
    highlow_identifier: str,
    force_overwrite: bool = False,
    data_dir: Optional[PathLike] = None,
) -> HighSideClient:
    """Initialize a high datasite with SyftBox configuration.

    Returns:
        HighSideClient: Initialized high-side client
    """
    return HighSideClient.initialize(
        email=email,
        highside_identifier=highlow_identifier,
        data_dir=data_dir,
        force_overwrite=force_overwrite,
    )
