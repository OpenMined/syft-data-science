import secrets
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from loguru import logger
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


def high_side_connect(email: str, data_dir: PathLike | None = None) -> SyftBoxClient:
    """Connect to a high datasite using the provided email."""
    if data_dir:
        data_dir = to_path(data_dir)
    else:
        data_dir = to_path(DEFAULT_HIGH_SIDE_DATA_DIR / email)

    syftbox_client = SyftBoxClient.load(data_dir / "config.json")
    logger.debug(
        f"Connected to high datasite {syftbox_client.email} at {syftbox_client.workspace.data_dir}"
    )

    return syftbox_client


def initialize_high_datasite(
    email: str,
    data_dir: Optional[PathLike] = None,
    force_overwrite: bool = False,
) -> Path:
    """Initialize a high datasite with SyftBox configuration."""
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

    client = SyftBoxClient(conf=syft_config)
    client.datasite_path.mkdir(parents=True, exist_ok=True)

    logger.debug(f"High datasite initialized successfully at: {data_dir}")
    return data_dir


def initialize_sync_config(
    lowside_syftbox_dir: PathLike,
    low_ssh_host: Optional[str] = None,
    low_ssh_user: Optional[str] = None,
    low_ssh_port: int = 22,
    low_ssh_key_path: Optional[PathLike] = None,
    force_overwrite: bool = False,
    highside_identifier: Optional[str] = None,
    add_default_entries: bool = True,
    syftbox_client: Optional[SyftBoxClient] = None,
) -> RsyncConfig:
    """Initialize sync configuration for a high datasite."""
    syftbox_client = syftbox_client or SyftBoxClient.load()

    lowside_syftbox_dir = to_path(lowside_syftbox_dir)
    low_ssh_key_path = to_path(low_ssh_key_path) if low_ssh_key_path else None

    highside_identifier = highside_identifier or _generate_high_side_name()
    logger.debug(f"Using high side identifier: {highside_identifier}")
    sync_config_path = get_rsync_config_path(syftbox_client)

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
        syftbox_client_email=syftbox_client.email,
        high_side_name=highside_identifier,
        low_syftbox_dir=lowside_syftbox_dir.resolve(),
        high_syftbox_dir=syftbox_client.workspace.data_dir,
        connection_settings=connection_settings,
        entries=[],
    )

    if add_default_entries:
        logger.debug("Adding default sync entries...")
        default_entries = _get_default_sync_entries(rsync_config)
        rsync_config.entries.extend(default_entries)

    rsync_config.save(syftbox_client)
    logger.debug(f"Sync configuration saved to {sync_config_path}")

    logger.debug("Initializing sync directories...")
    initialize_sync_dirs(rsync_config)

    return rsync_config


def initialize_sync_dirs(
    sync_config: RsyncConfig,
) -> None:
    for entry in sync_config.entries:
        entry.local_dir.mkdir(parents=True, exist_ok=True)


def get_sync_commands(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
    verbose: bool = True,
) -> list[str]:
    """Get rsync commands for configured sync entries, optionally filtered by direction."""
    syftbox_client = syftbox_client or SyftBoxClient.load()
    rsync_config = RsyncConfig.load(syftbox_client)

    if direction is None:
        commands: list[str] = rsync_config.get_rsync_commands()
        if verbose:
            for command in commands:
                logger.debug(_parse_rsync_command_for_display(command))
        return commands

    if isinstance(direction, str):
        direction = SyncDirection(direction)

    filtered_entries: list[RsyncEntry] = [
        entry for entry in rsync_config.entries if entry.direction == direction
    ]
    commands = []
    for entry in filtered_entries:
        command = entry.to_command(rsync_config.connection_settings)
        commands.append(command)
        if verbose:
            logger.info(f"Rsync command: {entry.show()}")

    return commands


def sync(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
    verbose: bool = True,
) -> None:
    """Execute rsync commands for configured sync entries, optionally filtered by direction."""
    syftbox_client = syftbox_client or SyftBoxClient.load()
    commands = get_sync_commands(
        direction, syftbox_client=syftbox_client, verbose=verbose
    )

    if not commands:
        logger.debug("No rsync commands to execute.")
        return

    for command in commands:
        subprocess.run(command, shell=True, check=True)

    return None


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


def _get_default_sync_entries(rsync_config: RsyncConfig) -> list[RsyncEntry]:
    """
    Creates the default sync entries between the low and high datasites.
    We call this from the high side
    - jobs: sync from low to high (low = remote => REMOTE_TO_LOCAL)
    - outputs: sync from high to low (high = local => LOCAL_TO_REMOTE)
    - datasets: sync the mock part of the dataset from high to low (high = local => LOCAL_TO_REMOTE)
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
        RsyncEntry(
            local_dir=rsync_config.public_dataset_dirs(Side.HIGH),
            remote_dir=rsync_config.public_dataset_dirs(Side.LOW),
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=False,
        ),
    ]


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
