from enum import StrEnum
from pathlib import Path
import subprocess
from typing import Union, Dict, Optional

from loguru import logger
from pydantic import BaseModel
from syft_core import Client as SyftBoxClient
from syft_core.types import PathLike
from syft_runtimes.consts import RUNTIME_CONFIG_FILE


class Side(StrEnum):
    HIGH = "high"
    LOW = "low"


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


class LocalConnection(BaseModel):
    lowside_syftbox_dir: Union[Path, str]


LowSideConnection = Union[LocalConnection, SSHConnection]


class RsyncEntry(BaseModel):
    local_dir: Path
    remote_dir: Path
    direction: SyncDirection
    ignore_existing: bool = True
    exclude_patterns: list[str] = []

    def to_command(self, connection: SSHConnection | None = None) -> str:
        return generate_rsync_command(self, connection)

    def show(self) -> str:
        exclude_info = (
            f" (excluding: {', '.join(self.exclude_patterns)})"
            if self.exclude_patterns
            else ""
        )
        return f"   → Syncing {self.direction} from {self.local_dir} to {self.remote_dir}{exclude_info}"


class RsyncConfig(BaseModel):
    # high_low_runtime_config: HighLowRuntimeConfig
    high_side_name: str
    high_syftbox_dir: Path
    low_syftbox_dir: Path
    connection_settings: SSHConnection | LocalConnection | None = None
    entries: list[RsyncEntry] = []
    syftbox_client_email: str | None = None

    @property
    def connection_type(self) -> ConnectionType:
        if self.connection_settings is None or isinstance(
            self.connection_settings, LocalConnection
        ):
            return ConnectionType.LOCAL
        return ConnectionType.SSH

    def path(self, syftbox_client: SyftBoxClient) -> Path:
        return get_rsync_config_path(syftbox_client)

    def save(self, syftbox_client: SyftBoxClient) -> None:
        self.path(syftbox_client=syftbox_client).write_text(
            self.model_dump_json(indent=2)
        )

    @classmethod
    def load(cls, syftbox_client: SyftBoxClient) -> "RsyncConfig":
        logger.debug(f"Loading rsync config from {syftbox_client.workspace.data_dir}")
        config_path = get_rsync_config_path(syftbox_client)
        if not config_path.exists():
            raise FileNotFoundError(
                f"High side sync config file not found at {config_path}"
            )

        return cls.model_validate_json(config_path.read_text())

    @classmethod
    def create_from_user_input(
        cls,
        high_side_name: str,
        high_syftbox_dir: Path,
        syftbox_client_email: str,
        lowside_data_dir: Optional[PathLike] = None,
        ssh_config: Optional[Dict] = None,
    ) -> "RsyncConfig":
        """
        Create RsyncConfig from user-friendly inputs.

        Args:
            high_side_name: Name of the high-side runtime
            high_syftbox_dir: Path to high-side SyftBox directory
            syftbox_client_email: Email of the SyftBox client
            lowside_data_dir: Path for local connections
            ssh_config: Dict for SSH connections

        Examples:
            # Local connection
            config = RsyncConfig.create_from_user_input(
                high_side_name="my-runtime",
                high_syftbox_dir="/path/to/high/syftbox",
                syftbox_client_email="user@example.com",
                lowside_data_dir="/path/to/low/syftbox"
            )

            # SSH connection
            config = RsyncConfig.create_from_user_input(
                high_side_name="my-runtime",
                high_syftbox_dir="/path/to/high/syftbox",
                syftbox_client_email="user@example.com",
                ssh_config={
                    "host": "lowside.example.com",
                    "user": "myuser",
                    "port": 2222,
                    "ssh_key_path": "~/.ssh/id_rsa"
                }
            )
        """
        connection = create_connection_from_config(lowside_data_dir, ssh_config)

        if isinstance(connection, LocalConnection):
            low_syftbox_dir = Path(connection.lowside_syftbox_dir)
            connection_settings = None
        else:  # SSHConnection
            low_syftbox_dir = Path(lowside_data_dir)
            connection_settings = connection

        return cls(
            high_side_name=high_side_name,
            high_syftbox_dir=Path(high_syftbox_dir),
            low_syftbox_dir=low_syftbox_dir,
            connection_settings=connection_settings,
            syftbox_client_email=syftbox_client_email,
        )

    def get_rsync_commands(self) -> list[str]:
        commands = []
        for entry in self.entries:
            command = entry.to_command(self.connection_settings)
            commands.append(command)
        return commands

    def high_low_runtime_dir(self, side: Side = Side.HIGH) -> Path:
        base_dir = self.high_syftbox_dir if side == Side.HIGH else self.low_syftbox_dir
        return (
            base_dir
            / "private"
            / self.syftbox_client_email
            / "syft_runtimes"
            / self.high_side_name
        )

    def jobs_dir(self, side: Side = Side.HIGH) -> Path:
        return self.high_low_runtime_dir(side) / "jobs"

    def outputs_dir(self, side: Side = Side.HIGH) -> Path:
        return self.high_low_runtime_dir(side) / "done"


class SyncResult(BaseModel):
    """Result of a sync operation."""

    commands_executed: int
    successful_syncs: int
    failed_syncs: int
    errors: list[str]

    @property
    def success(self) -> bool:
        return self.failed_syncs == 0


def get_rsync_config_path(
    syftbox_client: SyftBoxClient,
) -> Path:
    return syftbox_client.workspace.data_dir / "high_side_sync_config.json"


def generate_rsync_command(
    entry: RsyncEntry,
    connection: SSHConnection | None = None,
) -> str:
    # Base rsync flags: archive, verbose, progress, partial, human-readable
    flags = "-avP --human-readable --mkpath"
    if entry.ignore_existing:
        flags += " --ignore-existing"

    # Add exclude patterns
    for pattern in entry.exclude_patterns:
        flags += f" --exclude='{pattern}'"

    # Detect if we're syncing a file (has file extension) vs directory
    local_is_file = entry.local_dir.suffix != ""
    remote_is_file = entry.remote_dir.suffix != ""

    if connection is None:
        if entry.direction == SyncDirection.REMOTE_TO_LOCAL:
            source = str(entry.remote_dir)
            dest = str(entry.local_dir)
            # Only add trailing slash for directories
            if not remote_is_file:
                source += "/"
            if not local_is_file:
                dest += "/"
            return f"rsync {flags} {source} {dest}"
        else:
            source = str(entry.local_dir)
            dest = str(entry.remote_dir)
            # Only add trailing slash for directories
            if not local_is_file:
                source += "/"
            if not remote_is_file:
                dest += "/"
            return f"rsync {flags} {source} {dest}"
    else:
        # SSH rsync
        ssh_opts = f"-p {connection.port}"
        if connection.ssh_key_path:
            ssh_opts += f" -i {connection.ssh_key_path}"

        if entry.direction == SyncDirection.REMOTE_TO_LOCAL:
            remote_path = f"{connection.user}@{connection.host}:{entry.remote_dir}"
            local_path = str(entry.local_dir)
            if not remote_is_file:
                remote_path += "/"
            if not local_is_file:
                local_path += "/"
            return f"rsync {flags} -e 'ssh {ssh_opts}' {remote_path} {local_path}"
        else:
            local_path = str(entry.local_dir)
            remote_path = f"{connection.user}@{connection.host}:{entry.remote_dir}"
            if not local_is_file:
                local_path += "/"
            if not remote_is_file:
                remote_path += "/"
            return f"rsync {flags} -e 'ssh {ssh_opts}' {local_path} {remote_path}"


def create_connection_from_config(
    lowside_data_dir: Optional[PathLike] = None,
    ssh_config: Optional[Dict] = None,
) -> LowSideConnection:
    """
    Create connection object from user-friendly inputs.

    Args:
        lowside_data_dir: Path to local SyftBox directory (for local connections)
        ssh_config: Dict with SSH connection details (for remote connections)
            Required keys: 'host', 'user'
            Optional keys: 'port' (default: 22), 'ssh_key_path'

    Returns:
        LocalConnection or SSHConnection instance

    Examples:
        # Local connection
        conn = create_connection_from_config(lowside_data_dir="/path/to/syftbox")

        # SSH connection
        conn = create_connection_from_config(ssh_config={
            "host": "example.com",
            "user": "myuser",
            "port": 2222,  # optional
            "ssh_key_path": "/path/to/key"  # optional
        })
    """
    if ssh_config is None and lowside_data_dir is None:
        raise ValueError("Must specify either ssh_config or lowside_data_dir.")

    if ssh_config is not None:
        return create_ssh_connection(ssh_config)
    else:
        return create_local_connection(lowside_data_dir)


def create_ssh_connection(ssh_config: Dict) -> SSHConnection:
    """Convert user SSH config dict to SSHConnection object."""
    required_fields = {"host", "user"}
    provided_fields = set(ssh_config.keys())

    missing_fields = required_fields - provided_fields
    if missing_fields:
        raise ValueError(f"SSH config missing required fields: {missing_fields}")

    # Extract and validate fields
    host = ssh_config["host"]
    user = ssh_config["user"]
    port = ssh_config.get("port", 22)
    ssh_key_path = ssh_config.get("ssh_key_path")

    if ssh_key_path is not None:
        ssh_key_path = Path(ssh_key_path).expanduser()
        if not ssh_key_path.exists():
            logger.warning(f"SSH key path does not exist: {ssh_key_path}")

    return SSHConnection(host=host, port=port, user=user, ssh_key_path=ssh_key_path)


def create_local_connection(lowside_data_dir: PathLike) -> LocalConnection:
    """Convert user path input to LocalConnection object."""
    if not lowside_data_dir:
        raise ValueError("lowside_data_dir cannot be empty")

    path = Path(lowside_data_dir)
    if not path.exists():
        logger.warning(f"Local SyftBox directory does not exist: {path}")

    return LocalConnection(lowside_syftbox_dir=path)


def detect_connection_type(
    lowside_data_dir: Optional[PathLike] = None,
    ssh_config: Optional[Dict] = None,
) -> ConnectionType:
    """Detect connection type from user inputs."""
    if ssh_config is not None:
        return ConnectionType.SSH
    elif lowside_data_dir is not None:
        return ConnectionType.LOCAL
    else:
        raise ValueError("Must specify either ssh_config or lowside_data_dir.")


def _get_initial_sync_entries(rsync_config: RsyncConfig) -> list[RsyncEntry]:
    """
    Creates the initial sync entries to create the runtime structure on the low side
    We sync the runtime folder (`jobs/`, `done/` and `config.yaml`), except for the `running/` dir
    """

    return [
        RsyncEntry(
            local_dir=rsync_config.high_low_runtime_dir(Side.HIGH),
            remote_dir=rsync_config.high_low_runtime_dir(Side.LOW),
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=True,
            exclude_patterns=["running/"],  # Add this field to RsyncEntry
        )
    ]


def _get_default_sync_entries(rsync_config: RsyncConfig) -> list[RsyncEntry]:
    """
    Creates the default sync entries between the low and high datasites.
    We call this from the high side
    - jobs: sync from low to high (low = remote => REMOTE_TO_LOCAL)
    - outputs: sync from high to low (high = local => LOCAL_TO_REMOTE)
    - config.yaml: sync from high to low (high = local => LOCAL_TO_REMOTE)
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
            local_dir=rsync_config.high_low_runtime_dir(Side.HIGH)
            / RUNTIME_CONFIG_FILE,
            remote_dir=rsync_config.high_low_runtime_dir(Side.LOW)
            / RUNTIME_CONFIG_FILE,
            direction=SyncDirection.LOCAL_TO_REMOTE,
            ignore_existing=True,
        ),
    ]


def _get_sync_commands(
    rsync_config: RsyncConfig,
    direction: Optional[str | SyncDirection] = None,
    verbose: bool = True,
) -> list[str]:
    """Get rsync commands for configured sync entries, optionally filtered by direction."""
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
