from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel
from syft_core import Client as SyftBoxClient


class ConnectionType(StrEnum):
    SSH = "ssh"
    LOCAL = "local"


class SyncDirection(StrEnum):
    LOCAL_TO_REMOTE = "local_to_remote"
    REMOTE_TO_LOCAL = "remote_to_local"


class SSHConnection(BaseModel):
    host: str
    port: int = 22
    user: str
    ssh_key_path: Path | None = None


class RsyncEntry(BaseModel):
    local_dir: Path
    remote_dir: Path
    direction: SyncDirection
    ignore_existing: bool = True

    def to_command(self, connection: SSHConnection | None = None) -> str:
        return generate_rsync_command(self, connection)


class RsyncConfig(BaseModel):
    high_side_name: str
    high_syftbox_dir: Path
    low_syftbox_dir: Path
    connection_settings: SSHConnection | None = None
    entries: list[RsyncEntry] = []

    @property
    def connection_type(self) -> ConnectionType:
        if self.connection_settings is None:
            return ConnectionType.LOCAL
        return ConnectionType.SSH

    def save(self, syftbox_client: SyftBoxClient) -> None:
        config_path = get_rsync_config_path(syftbox_client)
        config_path.write_text(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, syftbox_client: SyftBoxClient) -> "RsyncConfig":
        config_path = get_rsync_config_path(syftbox_client)
        if not config_path.exists():
            raise FileNotFoundError(
                f"High side sync config file not found at {config_path}"
            )

        return cls.model_validate_json(config_path.read_text())

    def get_rsync_commands(self) -> list[str]:
        commands = []
        for entry in self.entries:
            command = entry.to_command(self.connection_settings)
            commands.append(command)
        return commands


def get_rsync_config_path(
    syftbox_client: SyftBoxClient,
) -> Path:
    return syftbox_client.workspace.data_dir / "high_side_sync_config.json"


def generate_rsync_command(
    entry: RsyncEntry,
    connection: SSHConnection | None = None,
) -> str:
    # Base rsync flags
    flags = "-av"
    if entry.ignore_existing:
        flags += " --ignore-existing"

    if connection is None:
        if entry.direction == SyncDirection.REMOTE_TO_LOCAL:
            return f"rsync {flags} {entry.remote_dir}/ {entry.local_dir}/"
        else:
            return f"rsync {flags} {entry.local_dir}/ {entry.remote_dir}/"
    else:
        # SSH rsync
        ssh_opts = f"-p {connection.port}"
        if connection.ssh_key_path:
            ssh_opts += f" -i {connection.ssh_key_path}"

        remote_path = f"{connection.user}@{connection.host}:{entry.remote_dir}/"

        if entry.direction == SyncDirection.REMOTE_TO_LOCAL:
            return f"rsync {flags} -e 'ssh {ssh_opts}' {remote_path} {entry.local_dir}/"
        else:
            return f"rsync {flags} -e 'ssh {ssh_opts}' {entry.local_dir}/ {remote_path}"
