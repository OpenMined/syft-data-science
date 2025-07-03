import secrets
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig
from syft_core.types import PathLike, to_path

from syft_runtimes.high_low.rsync import (
    RsyncConfig,
    RsyncEntry,
    SSHConnection,
    SyncDirection,
    get_rsync_config_path,
)


def high_side_connect(email: str, dir: PathLike | None = None) -> SyftBoxClient:
    """Connect to a high datasite using the provided email."""
    if dir:
        dir = to_path(dir)
    else:
        dir = to_path(Path.home() / ".syftbox" / "high-datasites" / email)

    syftbox_client = SyftBoxClient.load(dir / "config.json")
    print(
        f"Connected to high datasite {syftbox_client.email} at {syftbox_client.workspace.data_dir}"
    )

    return syftbox_client


def _generate_high_side_name() -> str:
    return f"high-side-{secrets.token_hex(4)}"


def _get_default_sync_entries(
    syftbox_client: SyftBoxClient, rsync_config: RsyncConfig
) -> list[RsyncEntry]:
    private_dir = Path("private")
    relative_highside_dir = private_dir / "job_runners" / rsync_config.high_side_name
    relative_jobs_dir = relative_highside_dir / "jobs"
    relative_outputs_dir = relative_highside_dir / "outputs"
    relative_datasets_dir = relative_highside_dir / "datasets"

    low_syftbox_dir = rsync_config.low_syftbox_dir
    high_syftbox_dir = syftbox_client.workspace.data_dir

    return [
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_jobs_dir,
            remote_dir=low_syftbox_dir / relative_jobs_dir,
            direction=SyncDirection.REMOTE_TO_LOCAL,
            ignore_existing=True,
        ),
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_outputs_dir,
            remote_dir=low_syftbox_dir / relative_outputs_dir,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=False,
        ),
        RsyncEntry(
            local_dir=high_syftbox_dir / relative_datasets_dir,
            remote_dir=low_syftbox_dir / relative_datasets_dir,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=True,
        ),
    ]


def initialize_high_datasite(
    email: str,
    dir: Optional[Path] = None,
    force_overwrite: bool = False,
) -> Path:
    """Initialize a high datasite with SyftBox configuration."""
    dir = dir or Path.home() / ".syftbox" / "high-datasites" / email

    if dir.exists() and not force_overwrite:
        raise FileExistsError(
            f"Directory {dir} already exists. Use force_overwrite=True to reset."
        )

    if force_overwrite and dir.exists():
        print(f"Overwriting existing directory: {dir}")
        shutil.rmtree(dir)

    print(f"Creating directory: {dir}")
    dir.mkdir(parents=True, exist_ok=True)
    config_path = dir / "config.json"
    data_dir = dir / "SyftBox"

    print(f"Saving SyftBox configuration to: {config_path}")
    syft_config = SyftClientConfig(
        email=email,
        client_url="http://testserver:5000",
        path=config_path,
        data_dir=data_dir,
    )
    syft_config.save()

    client = SyftBoxClient(conf=syft_config)
    client.datasite_path.mkdir(parents=True, exist_ok=True)

    print(f"High datasite initialized successfully at: {data_dir}")
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
    print(f"Using high side identifier: {highside_identifier}")
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
        high_side_name=highside_identifier,
        low_syftbox_dir=lowside_syftbox_dir.resolve(),
        high_syftbox_dir=syftbox_client.workspace.data_dir,
        connection_settings=connection_settings,
        entries=[],
    )

    if add_default_entries:
        print("Adding default sync entries...")
        default_entries = _get_default_sync_entries(syftbox_client, rsync_config)
        rsync_config.entries.extend(default_entries)
    rsync_config.save(syftbox_client)
    print(f"Sync configuration saved to {sync_config_path}")

    print("Initializing sync directories...")
    initialize_sync_dirs(rsync_config)
    rsync_config.save(syftbox_client)
    return rsync_config


def initialize_sync_dirs(
    sync_config: RsyncConfig,
) -> None:
    for entry in sync_config.entries:
        entry.local_dir.mkdir(parents=True, exist_ok=True)


def get_sync_commands(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
) -> list[str]:
    """Get rsync commands for configured sync entries, optionally filtered by direction."""
    syftbox_client = syftbox_client or SyftBoxClient.load()
    rsync_config = RsyncConfig.load(syftbox_client)

    if direction is None:
        return rsync_config.get_rsync_commands()

    if isinstance(direction, str):
        direction = SyncDirection(direction)

    filtered_entries = [
        entry for entry in rsync_config.entries if entry.direction == direction
    ]
    commands = []
    for entry in filtered_entries:
        command = entry.to_command(rsync_config.connection_settings)
        commands.append(command)

    return commands


def show_rsync_commands(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
) -> None:
    """Print rsync commands for configured sync entries, optionally filtered by direction."""
    syftbox_client = syftbox_client or SyftBoxClient.load()
    commands = get_sync_commands(direction, syftbox_client=syftbox_client)
    if not commands:
        print("No rsync commands found.")
        return

    print("Rsync Commands:")
    for command in commands:
        print(f"{command}")


def sync(
    direction: Optional[str | SyncDirection] = None,
    syftbox_client: Optional[SyftBoxClient] = None,
) -> None:
    """Execute rsync commands for configured sync entries, optionally filtered by direction."""
    syftbox_client = syftbox_client or SyftBoxClient.load()
    commands = get_sync_commands(direction, syftbox_client=syftbox_client)
    if not commands:
        print("No rsync commands to execute.")
        return

    for command in commands:
        print(f"Executing: {command}")
        subprocess.run(command, shell=True, check=True)
