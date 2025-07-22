import secrets
import shutil
from typing import Optional

from loguru import logger
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike, to_path

from syft_runtimes.high_low.rsync import (
    RsyncConfig,
    RsyncEntry,
    Side,
    SSHConnection,
    SyncDirection,
    get_rsync_config_path,
)
from syft_runtimes.high_low.consts import DEFAULT_HIGH_SIDE_DATA_DIR
from syft_runtimes.models import BaseRuntimeConfig
from syft_runtimes.high_low.lowside_client import LowSideClient


class HighLowRuntimeConfig(BaseRuntimeConfig):
    """Configuration for high-low runtime with dataset tracking."""

    def add_dataset(self, dataset_name: str) -> bool:
        """Add a dataset to the config if not already present.

        Returns:
            True if dataset was added, False if already present
        """
        if dataset_name not in self.datasets:
            self.datasets.append(dataset_name)
            self.save_to_yaml()
            return True
        return False

    def remove_dataset(self, dataset_name: str) -> bool:
        """Remove a dataset from the config.

        Returns:
            True if dataset was removed, False if not found
        """
        if dataset_name in self.datasets:
            self.datasets.remove(dataset_name)
            self.save_to_yaml()
            return True
        return False


def _create_default_sync_config(
    highside_client: SyftBoxClient,
    lowside_client: LowSideClient,
    highside_identifier: Optional[str] = None,
    low_ssh_host: Optional[str] = None,
    low_ssh_user: Optional[str] = None,
    low_ssh_port: int = 22,
    low_ssh_key_path: Optional[PathLike] = None,
    force_overwrite: bool = False,
) -> RsyncConfig:
    """Create a sync configuration with default sync entries for jobs, outputs, and datasets."""
    rsync_config = _create_empty_sync_config(
        highside_client=highside_client,
        lowside_client=lowside_client,
        highside_identifier=highside_identifier,
        low_ssh_host=low_ssh_host,
        low_ssh_user=low_ssh_user,
        low_ssh_port=low_ssh_port,
        low_ssh_key_path=low_ssh_key_path,
        force_overwrite=force_overwrite,
    )

    # Add default entries
    default_entries: list[RsyncEntry] = _get_default_sync_entries(rsync_config)
    rsync_config.entries.extend(default_entries)
    rsync_config.save(highside_client)
    logger.debug(f"Sync configuration saved to {rsync_config.path(highside_client)}")

    return rsync_config


def _get_default_sync_entries():
    pass


def _get_sync_commands():
    pass


def sync(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
    rsync_config: Optional[RsyncConfig] = None,
    verbose: bool = True,
) -> None:
    """Execute rsync commands for runtime folders (jobs and outputs only)."""
    rsync_config = rsync_config or _load_client_sync_config(syftbox_client)

    # Filter config to only include runtime operations (jobs and outputs)
    runtime_rsync_config = _filter_runtime_sync_entries(rsync_config)
    _validate_runtime_sync_config(runtime_rsync_config)
    _initialize_sync_dirs(runtime_rsync_config)

    commands: list[str] = _get_sync_commands(
        rsync_config=runtime_rsync_config, direction=direction, verbose=verbose
    )

    if not commands:
        logger.debug("No runtime sync commands to execute.")
        return

    # _execute_sync_commands(commands, verbose)

    return None


def sync_dataset(
    dataset_name: str,
    highside_client: SyftBoxClient,
    lowside_client: LowSideClient,
    verbose: bool = True,
) -> None:
    """Sync the public part of a specific dataset from high-side to low-side.

    Args:
        dataset_name: Name of the dataset to sync
        highside_client: The high-side syftbox client
        lowside_client: The low-side syftbox client
        verbose: print more info if True
    """
    # Get dataset directories on high and low sides
    high_dataset_dir = (
        highside_client.my_datasite / "public" / "syft_datasets" / dataset_name
    )
    low_dataset_dir = lowside_client.get_dataset_path(dataset_name, private=False)
    rsync_config = _load_client_sync_config(highside_client)

    # Validate that the dataset exists on high-side
    if not high_dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset '{dataset_name}' not found on high-side at {high_dataset_dir}"
        )

    # Create the dataset sync entry
    dataset_entry = RsyncEntry(
        local_dir=high_dataset_dir,
        remote_dir=low_dataset_dir,
        direction=SyncDirection.LOCAL_TO_REMOTE,
        ignore_existing=False,  # Overwrite existing files to ensure updates
    )

    # Ensure the parent directory exists on low-side
    lowside_client.ensure_directory_exists(low_dataset_dir.parent)

    # Generate and execute the rsync command
    command = dataset_entry.to_command(rsync_config.connection_settings)
    print(command)

    if verbose:
        logger.info(f"Syncing dataset '{dataset_name}' from high-side to low-side")
        logger.info(f"Source: {high_dataset_dir}")
        logger.info(f"Destination: {low_dataset_dir}")
        # logger.debug(_parse_rsync_command_for_display(command))

    # _execute_sync_commands([command], verbose=verbose)

    _add_dataset_name_to_config(dataset_name, highside_client, Side.HIGH, rsync_config)
    # For local connections, also update the low-side config
    if lowside_client.is_local():
        local_client = (
            lowside_client.syftbox_client
        )  # Access the underlying SyftBoxClient
        _add_dataset_name_to_config(dataset_name, local_client, Side.LOW, rsync_config)

    return None


def _generate_high_side_name() -> str:
    return f"high-side-{secrets.token_hex(4)}"


def _init_highside_data_dir(
    email: str, data_dir: PathLike | None, force_overwrite: bool = False
) -> PathLike:
    if data_dir:
        data_dir = to_path(data_dir)
    else:
        data_dir = to_path(DEFAULT_HIGH_SIDE_DATA_DIR / email)

    if data_dir.exists() and not force_overwrite:
        raise FileExistsError(
            f"Directory {data_dir} already exists. Use force_overwrite=True to reset."
        )

    if force_overwrite and data_dir.exists():
        logger.debug(f"Overwriting existing directory: {data_dir}")
        shutil.rmtree(data_dir)

    logger.debug(f"Creating directory: {data_dir}")
    data_dir.mkdir(parents=True, exist_ok=True)

    return data_dir


def _create_empty_sync_config(
    highside_client: SyftBoxClient,
    lowside_client: LowSideClient,
    highside_identifier: Optional[str] = None,
    low_ssh_host: Optional[str] = None,
    low_ssh_user: Optional[str] = None,
    low_ssh_port: int = 22,
    low_ssh_key_path: Optional[PathLike] = None,
    force_overwrite: bool = False,
) -> RsyncConfig:
    """Create empty sync configurations"""
    # Get the appropriate data directories based on client type
    if lowside_client.is_local():
        lowside_syftbox_dir = to_path(lowside_client.syftbox_client.workspace.data_dir)
    else:
        # For SSH clients, use the parent of the datasite path
        lowside_syftbox_dir = lowside_client.datasite_path.parent

    highside_syftbox_dir = to_path(highside_client.workspace.data_dir)

    low_ssh_key_path = to_path(low_ssh_key_path) if low_ssh_key_path else None

    highside_identifier = highside_identifier or _generate_high_side_name()
    logger.debug(f"Using high side identifier: {highside_identifier}")
    sync_config_path = get_rsync_config_path(highside_client)

    if sync_config_path.exists() and not force_overwrite:
        raise FileExistsError(
            f"Sync config already exists at {sync_config_path}. Use force_overwrite=True to replace."
        )

    connection_settings = None
    if low_ssh_host:
        if not low_ssh_user:
            raise ValueError("SSH user is required when using SSH")
        connection_settings = SSHConnection(
            host=low_ssh_host,
            user=low_ssh_user,
            port=low_ssh_port,
            ssh_key_path=low_ssh_key_path,
        )

    rsync_config = RsyncConfig(
        syftbox_client_email=highside_client.email,
        high_side_name=highside_identifier,
        low_syftbox_dir=lowside_syftbox_dir.resolve(),
        high_syftbox_dir=highside_syftbox_dir.resolve(),
        connection_settings=connection_settings,
        entries=[],
    )

    return rsync_config


# def _get_default_sync_entries(rsync_config: RsyncConfig) -> list[RsyncEntry]:
#     """
#     Creates the default sync entries between the low and high datasites.
#     We call this from the high side
#     - jobs: sync from low to high (low = remote => REMOTE_TO_LOCAL)
#     - outputs: sync from high to low (high = local => LOCAL_TO_REMOTE)
#     """

#     return [
#         RsyncEntry(
#             local_dir=rsync_config.jobs_dir(Side.HIGH),
#             remote_dir=rsync_config.jobs_dir(Side.LOW),
#             direction=SyncDirection.REMOTE_TO_LOCAL,
#             ignore_existing=True,
#         ),
#         RsyncEntry(
#             local_dir=rsync_config.outputs_dir(Side.HIGH),
#             remote_dir=rsync_config.outputs_dir(Side.LOW),
#             direction=SyncDirection.LOCAL_TO_REMOTE,
#             ignore_existing=False,
#         ),
#     ]


def _validate_runtime_sync_config(rsync_config: RsyncConfig) -> None:
    """Validate runtime sync operations (jobs and outputs only)."""
    if not rsync_config.entries:
        raise ValueError("Runtime sync configuration is empty")

    allowed_operations = {
        "jobs": {
            "direction": SyncDirection.REMOTE_TO_LOCAL,
            "path_suffix": "/jobs",
            "description": "jobs from low to high",
        },
        "outputs": {
            "direction": SyncDirection.LOCAL_TO_REMOTE,
            "path_suffix": "/done",
            "description": "outputs from high to low",
        },
    }

    for i, entry in enumerate(rsync_config.entries):
        operation_found = False

        for op_spec in allowed_operations.values():
            if (
                entry.direction == op_spec["direction"]
                and str(entry.local_dir).endswith(op_spec["path_suffix"])
                and str(entry.remote_dir).endswith(op_spec["path_suffix"])
            ):
                operation_found = True
                logger.debug(f"Runtime entry {i+1}: Valid {op_spec['description']}")
                break

        if not operation_found:
            entry_description = f"direction={entry.direction.value}, local_dir={entry.local_dir}, remote_dir={entry.remote_dir}"
            allowed_descriptions = [
                f"- {spec['description']}" for spec in allowed_operations.values()
            ]

            raise ValueError(
                f"Invalid runtime sync operation in entry {i+1}: {entry_description}. "
                f"Only these runtime operations are allowed:\n"
                + "\n".join(allowed_descriptions)
            )


def _filter_runtime_sync_entries(rsync_config: RsyncConfig) -> RsyncConfig:
    """Filter sync config to only include runtime operations (jobs and outputs)."""
    runtime_entries = [
        entry
        for entry in rsync_config.entries
        if (
            str(entry.local_dir).endswith("/jobs")
            or str(entry.local_dir).endswith("/done")
        )
    ]

    # Create a new config with only runtime entries
    filtered_config = RsyncConfig(
        high_side_name=rsync_config.high_side_name,
        high_syftbox_dir=rsync_config.high_syftbox_dir,
        low_syftbox_dir=rsync_config.low_syftbox_dir,
        connection_settings=rsync_config.connection_settings,
        entries=runtime_entries,
        syftbox_client_email=rsync_config.syftbox_client_email,
    )

    return filtered_config


def _initialize_sync_dirs(
    sync_config: RsyncConfig,
) -> None:
    """Create all required sync directories if they don't exist."""
    logger.debug("Ensuring sync directories exist...")

    for entry in sync_config.entries:
        try:
            entry.local_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Directory ready: {entry.local_dir}")
        except OSError as e:
            logger.error(f"Failed to create directory {entry.local_dir}: {e}")
            raise


def _add_dataset_name_to_config(
    dataset_name: str,
    client: SyftBoxClient,
    side: Side,
    rsync_config: Optional[RsyncConfig] = None,
) -> None:
    """Add a dataset name to the runtime config.yaml file if not already present.

    Args:
        dataset_name: Name of the dataset to add to config
        client: The syftbox client
        side: Side of the runtime config to update (HIGH or LOW)
        rsync_config: Optional rsync config, will load if not provided
    """
    if rsync_config is None:
        rsync_config = _load_client_sync_config(client)

    # Get the runtime directory using the existing utility
    runtime_dir = rsync_config.high_low_runtime_dir(side=side)
    runtime_config_path = runtime_dir / "config.yaml"

    try:
        # Try to load existing config
        if runtime_config_path.exists():
            runtime_config = HighLowRuntimeConfig.from_yaml(runtime_config_path)
        else:
            # Create new config
            runtime_config = HighLowRuntimeConfig(
                config_path=runtime_config_path, datasets=[dataset_name]
            )
    except Exception as e:
        logger.warning(f"Failed to load existing config, creating new one: {e}")
        runtime_config = HighLowRuntimeConfig(
            config_path=runtime_config_path, datasets=[dataset_name]
        )

    # Add dataset using the model's method
    if runtime_config.add_dataset(dataset_name):
        logger.info(
            f"Added dataset '{dataset_name}' to runtime config at {runtime_config_path}"
        )
    else:
        logger.debug(
            f"Dataset '{dataset_name}' already exists in runtime config at {runtime_config_path}"
        )


def _load_client_sync_config(syftbox_client: Optional[SyftBoxClient]) -> RsyncConfig:
    """Load rsync configuration from the syftbox_client's high_side_sync_config.json file"""
    client = syftbox_client or SyftBoxClient.load()

    try:
        return RsyncConfig.load(client)
    except FileNotFoundError as e:
        logger.error(f"Sync configuration not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to load sync configuration: {e}")
        raise
