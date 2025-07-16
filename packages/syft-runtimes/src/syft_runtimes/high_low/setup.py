import secrets
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from loguru import logger
from pydantic import BaseModel
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_core.types import PathLike, to_path
from syft_datasets import Dataset
from syft_datasets.dataset_manager import SyftDatasetManager

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
from syft_runtimes.runners import HighLowRuntime


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


class SyncResult(BaseModel):
    """Result of a sync operation."""

    commands_executed: int
    successful_syncs: int
    failed_syncs: int
    errors: list[str]

    @property
    def success(self) -> bool:
        return self.failed_syncs == 0


def initialize_high_datasite(
    highside_identifier: str,
    force_overwrite: bool = False,
    highside_data_dir: Optional[PathLike] = None,
    lowside_syftbox_client: Optional[SyftBoxClient] = None,
) -> Path:
    """Initialize a high datasite with SyftBox configuration."""
    lowside_syftbox_client = lowside_syftbox_client or SyftBoxClient.load()
    email = lowside_syftbox_client.email
    highside_data_dir = _init_highside_data_dir(
        email, highside_data_dir, force_overwrite
    )
    highside_client = _init_highside_client(email, highside_data_dir)
    _init_highlow_runtime(highside_client, highside_identifier, lowside_syftbox_client)

    return highside_data_dir


def high_side_connect(email: str, data_dir: PathLike | None = None) -> SyftBoxClient:
    """Connect to a high datasite using the provided email."""
    if data_dir:
        data_dir = to_path(data_dir)
    else:
        data_dir = to_path(DEFAULT_HIGH_SIDE_DATA_DIR / email)

    config_path = data_dir / "config.json"
    syftbox_client = SyftBoxClient.load(config_path)
    if syftbox_client.email != email:
        raise ValueError(
            f"Provided email ({email}) does not match the email in the config file at {config_path}, which is "
        )

    logger.debug(
        f"Connected to high datasite {syftbox_client.email} at {syftbox_client.workspace.data_dir}"
    )

    return syftbox_client


def create_default_sync_config(
    highside_client: SyftBoxClient,
    lowside_client: SyftBoxClient,
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

    _execute_sync_commands(commands, verbose)

    return None


def sync_dataset(
    dataset_name: str,
    highside_client: SyftBoxClient,
    lowside_client: SyftBoxClient,
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
    low_dataset_dir = (
        lowside_client.my_datasite / "public" / "syft_datasets" / dataset_name
    )
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
    low_dataset_dir.parent.mkdir(parents=True, exist_ok=True)

    # Generate and execute the rsync command
    command = dataset_entry.to_command(rsync_config.connection_settings)

    if verbose:
        logger.info(f"Syncing dataset '{dataset_name}' from high-side to low-side")
        logger.info(f"Source: {high_dataset_dir}")
        logger.info(f"Destination: {low_dataset_dir}")
        logger.debug(_parse_rsync_command_for_display(command))

    _execute_sync_commands([command], verbose=verbose)

    _add_dataset_name_to_config(dataset_name, highside_client, Side.HIGH, rsync_config)
    _add_dataset_name_to_config(dataset_name, lowside_client, Side.LOW, rsync_config)

    return None


def _execute_sync_commands(commands: list[str], verbose: bool) -> SyncResult:
    """Execute rsync commands and return detailed results."""

    if not commands:
        logger.info("No sync commands to execute")
        return SyncResult(
            commands_executed=0, successful_syncs=0, failed_syncs=0, errors=[]
        )

    logger.info(f"Executing {len(commands)} sync command(s)")

    num_success = 0
    errors = []

    for i, command in enumerate(commands, 1):
        try:
            logger.debug(f"Executing command {i}/{len(commands)}")

            # Safer command execution - split command properly
            result = subprocess.run(
                command,
                shell=True,  # Still need shell for rsync with SSH
                check=True,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            num_success += 1
            if verbose:
                logger.info(f"✓ Sync command {i} completed successfully")

        except subprocess.TimeoutExpired:
            error_msg = f"Command {i} timed out after 5 minutes"
            logger.error(error_msg)
            errors.append(error_msg)

        except subprocess.CalledProcessError as e:
            error_msg = f"Command {i} failed with exit code {e.returncode}: {e.stderr}"
            logger.error(error_msg)
            errors.append(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error in command {i}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    num_failed = len(commands) - num_success
    result = SyncResult(
        commands_executed=len(commands),
        successful_syncs=num_success,
        failed_syncs=num_failed,
        errors=errors,
    )

    if result.success:
        logger.info(f"✅ All {num_success} sync operations completed successfully")
    else:
        logger.error(f"❌ {num_failed} of {len(commands)} sync operations failed")

    return result


def prepare_dataset_for_low_side(
    dataset: Dataset,
    syftbox_client: Optional[SyftBoxClient] = None,
) -> None:
    sync_config = RsyncConfig.load(syftbox_client or SyftBoxClient.load())
    mock_dir = dataset.mock_dir
    logger.debug(
        f"Copying dataset {dataset.name} to {sync_config.public_dataset_dirs(Side.HIGH)}"
    )

    shutil.copytree(
        mock_dir,
        sync_config.public_dataset_dirs(Side.HIGH) / dataset.name,
        dirs_exist_ok=True,
    )


def prepare_datasets_from_high_side(
    high_side_name: str,
    overwrite: bool = False,
    syftbox_client: SyftBoxClient | None = None,
) -> None:
    syftbox_client = syftbox_client or SyftBoxClient.load()
    high_side_runtime_folder = (
        syftbox_client.workspace.data_dir
        / "private"
        / syftbox_client.email
        / "syft_runtimes"
        / high_side_name
    )
    if not high_side_runtime_folder.is_dir():
        raise ValueError(
            f"High side runtime folder {high_side_runtime_folder} does not exist or is not a directory."
        )
    high_datasets_dir = high_side_runtime_folder / "datasets"
    if not high_datasets_dir.is_dir():
        raise ValueError(
            f"High side datasets directory {high_datasets_dir} does not exist or is not a directory."
        )

    dataset_manager = SyftDatasetManager(syftbox_client=syftbox_client)
    local_mock_datasets_dir = dataset_manager.public_dir_for_datasite(
        syftbox_client.email
    )

    # Foreach dataset in the high side datasets directory,
    # check if it already exists (warning) and if not, copy it to the local mock datasets directory.
    for dataset_path in high_datasets_dir.iterdir():
        if not dataset_path.is_dir():
            continue
        dataset_name = dataset_path.name
        local_dataset_path = local_mock_datasets_dir / dataset_name
        if local_dataset_path.exists() and overwrite is False:
            # If the dataset already exists, skip copying it.
            logger.debug(
                f"Dataset {dataset_name} already exists in local mock datasets directory. Skipping copy."
            )
            continue
        logger.debug(
            f"Copying dataset {dataset_name} to local mock datasets directory."
        )
        logger.debug(
            f"Copying dataset {dataset_name}: source: {dataset_path}, destination: {local_dataset_path}"
        )
        shutil.copytree(
            dataset_path,
            local_dataset_path,
            dirs_exist_ok=True,
        )


def _generate_high_side_name() -> str:
    return f"high-side-{secrets.token_hex(4)}"


def _init_highlow_runtime(
    highside_client: SyftBoxClient,
    highside_identifier: str,
    lowside_syftbox_client: SyftBoxClient,
) -> Path:
    high_low_runtime = HighLowRuntime(
        highside_client, highside_identifier, lowside_syftbox_client
    )
    high_low_runtime.init_runtime_dir()


def _init_highside_client(email: str, data_dir: PathLike) -> SyftBoxClient:
    config_path = data_dir / "config.json"
    data_dir = data_dir / "SyftBox"

    logger.debug(f"Saving SyftBox configuration to: {config_path}")
    syft_config = SyftClientConfig(
        email=email,
        client_url="http://testserver:5000",
        path=config_path,
        data_dir=data_dir,
    )
    syft_config.save()

    high_side_client = SyftBoxClient(conf=syft_config)
    high_side_client.datasite_path.mkdir(parents=True, exist_ok=True)

    return high_side_client


def _init_highside_data_dir(
    email: str, data_dir: PathLike | None = None, force_overwrite: bool = False
) -> PathLike:
    data_dir = data_dir or DEFAULT_HIGH_SIDE_DATA_DIR / email

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
    lowside_client: SyftBoxClient,
    highside_identifier: Optional[str] = None,
    low_ssh_host: Optional[str] = None,
    low_ssh_user: Optional[str] = None,
    low_ssh_port: int = 22,
    low_ssh_key_path: Optional[PathLike] = None,
    force_overwrite: bool = False,
) -> RsyncConfig:
    """Create empty sync configurations"""
    lowside_syftbox_dir = to_path(lowside_client.workspace.data_dir)
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


def _get_default_sync_entries(rsync_config: RsyncConfig) -> list[RsyncEntry]:
    """
    Creates the default sync entries between the low and high datasites.
    We call this from the high side
    - jobs: sync from low to high (low = remote => REMOTE_TO_LOCAL)
    - outputs: sync from high to low (high = local => LOCAL_TO_REMOTE)
    """

    return [
        RsyncEntry(
            local_dir=rsync_config.jobs_dir(Side.HIGH),
            remote_dir=rsync_config.jobs_dir(Side.LOW),
            direction=SyncDirection.REMOTE_TO_LOCAL,
            ignore_existing=True,
        ),
        RsyncEntry(
            local_dir=rsync_config.outputs_dir(Side.HIGH),
            remote_dir=rsync_config.outputs_dir(Side.LOW),
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=False,
        ),
    ]


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


def _validate_sync_direction(
    direction: Optional[str | SyncDirection],
) -> Optional[SyncDirection]:
    """Validate and convert direction parameter to SyncDirection enum."""
    if direction is None:
        return None

    if isinstance(direction, str):
        try:
            return SyncDirection(direction)
        except ValueError as e:
            raise ValueError(f"Invalid sync direction: {direction}") from e


def _get_sync_commands(
    rsync_config: RsyncConfig,
    direction: Optional[str | SyncDirection] = None,
    verbose: bool = True,
) -> list[str]:
    """Get rsync commands for configured sync entries, optionally filtered by direction."""
    direction = _validate_sync_direction(direction)

    if direction is None:
        commands: list[str] = rsync_config.get_rsync_commands()
        if verbose:
            for command in commands:
                logger.debug(_parse_rsync_command_for_display(command))
        return commands

    filtered_entries: list[RsyncEntry] = [
        entry for entry in rsync_config.entries if entry.direction == direction
    ]
    commands: list[str] = []

    for entry in filtered_entries:
        command = entry.to_command(rsync_config.connection_settings)
        commands.append(command)
        if verbose:
            logger.info(f"Rsync command: {entry.show()}")

    return commands


def _parse_rsync_command_for_display(command: str) -> str:
    """Parse rsync command to create human-readable description."""

    # Extract source and destination from the command
    # rsync -avP --human-readable --ignore-existing /path/source/ /path/dest/
    parts = command.split()

    # Find source and destination (last two non-flag arguments)
    paths = [part for part in parts if not part.startswith("-") and part != "rsync"]
    if len(paths) < 2:
        return "Unknown sync operation"

    source, dest = paths[-2], paths[-1]

    return f"   → Syncing from {source} to {dest}"
