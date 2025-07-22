from abc import ABC, abstractmethod
import subprocess
from pathlib import Path
from loguru import logger

from syft_core import Client as SyftBoxClient

from syft_runtimes.high_low.rsync import ConnectionType
from syft_runtimes.runtimes.base import Runtime
from syft_runtimes.runtimes.runtime_factory import (
    create_local_runtime,
    create_ssh_runtime,
)


class LowSideClient(ABC):
    """Abstract base class for low-side clients (local and SSH)."""

    def __init__(self, email: str, connection_type: ConnectionType):
        self.email = email
        self.connection_type = connection_type

    @property
    @abstractmethod
    def datasite_path(self) -> Path:
        """Get the path to the datasite directory."""
        pass

    @property
    def my_datasite(self) -> Path:
        """Get the path to this user's datasite."""
        return self.datasite_path / self.email

    @property
    def data_dir(self) -> Path:
        """Get the path to the SyftBox directory."""
        pass

    @abstractmethod
    def ensure_directory_exists(self, path: Path) -> bool:
        """Ensure a directory exists, creating it if necessary."""
        pass

    @property
    @abstractmethod
    def runtime(self) -> Runtime:
        """Get the runtime instance for this client."""
        pass

    def initialize_runtime_directories(self, highlow_identifier: str) -> bool:
        """Initialize the runtime directory structure using the runtime."""
        try:
            runtime = self.create_runtime(highlow_identifier)
            runtime_dir = runtime.init_runtime_dir()
            logger.debug(f"Runtime initialized at: {runtime_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize runtime directories: {e}")
            return False

    @abstractmethod
    def create_runtime(self, highlow_identifier: str) -> Runtime:
        """Create the appropriate runtime for this client."""
        pass

    @abstractmethod
    def get_mock_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        pass

    @abstractmethod
    def get_private_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        pass

    @abstractmethod
    def get_runtime_path(self, highlow_identifier: str) -> Path:
        """Get the path for a specific runtime."""
        pass

    def is_local(self) -> bool:
        """Check if this is a local connection."""
        return self.connection_type == ConnectionType.LOCAL

    def is_ssh(self) -> bool:
        """Check if this is an SSH connection."""
        return self.connection_type == ConnectionType.SSH


class LocalLowSideClient(LowSideClient):
    """Local low-side client using direct file operations."""

    def __init__(self, syftbox_client: SyftBoxClient):
        super().__init__(syftbox_client.email, ConnectionType.LOCAL)
        self.syftbox_client = syftbox_client

    @property
    def datasite_path(self) -> Path:
        """Get the local datasite path."""
        return self.syftbox_client.workspace.data_dir / "datasites"

    @property
    def data_dir(self) -> Path:
        """Get the path to the SyftBox directory."""
        return self.syftbox_client.workspace.data_dir

    def ensure_directory_exists(self, path: Path) -> bool:
        """Create directory locally."""
        try:
            path.mkdir(parents=True, exist_ok=True)
            return True
        except OSError as e:
            logger.error(f"Failed to create directory {path}: {e}")
            return False

    def create_runtime(self, highlow_identifier: str) -> Runtime:
        """Create a local runtime."""
        self.runtime = create_local_runtime(self.syftbox_client, highlow_identifier)
        return self.runtime

    def get_runtime_path(self, highlow_identifier: str) -> Path:
        """Get runtime path."""
        return self.data_dir / "private" / "syft_runtimes" / highlow_identifier

    def get_mock_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        return (
            self.syftbox_client.my_datasite / "public" / "syft_datasets" / dataset_name
        )

    def get_private_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        return (
            self.syftbox_client.workspace.data_dir
            / "private"
            / "syft_datasets"
            / dataset_name
        )

    def runtime(self) -> Runtime:
        """Get the runtime instance for this client."""
        return self.runtime


class SSHLowSideClient(LowSideClient):
    """SSH low-side client using remote operations."""

    def __init__(self, email: str, ssh_config: dict, remote_syftbox_dir: Path):
        super().__init__(email, ConnectionType.SSH)
        self.ssh_config = ssh_config
        self.remote_syftbox_dir = remote_syftbox_dir
        self.connection_string = f"{ssh_config['user']}@{ssh_config['host']}"

    @property
    def datasite_path(self) -> Path:
        """Get the remote datasite path."""
        return self.remote_syftbox_dir / "datasites"

    @property
    def data_dir(self) -> Path:
        """Get the path to the SyftBox directory."""
        return self.remote_syftbox_dir

    def ensure_directory_exists(self, path: Path) -> bool:
        """Create directory on remote machine via SSH."""
        command = f"mkdir -p {path}"
        stdout, stderr, exit_code = self._execute_ssh_command(command)
        if exit_code != 0:
            logger.error(f"Failed to create remote directory {path}: {stderr}")
            return False
        return True

    def create_runtime(self, highlow_identifier: str) -> Runtime:
        """Create an SSH runtime."""
        self.runtime = create_ssh_runtime(
            runtime_name=highlow_identifier,
            ssh_config=self.ssh_config,
            remote_base_dir=self.remote_syftbox_dir,
        )
        return self.runtime

    def runtime(self) -> Runtime:
        """Get the runtime instance for this client."""
        return self.runtime

    def get_runtime_path(self, highlow_identifier: str) -> Path:
        """Get remote runtime path."""
        return self.data_dir / "private" / "syft_runtimes" / highlow_identifier

    def _execute_ssh_command(self, command: str) -> tuple[str, str, int]:
        """Execute SSH command and return stdout, stderr, exit_code."""
        ssh_cmd = ["ssh"]

        if self.ssh_config.get("ssh_key_path"):
            ssh_cmd.extend(["-i", str(self.ssh_config["ssh_key_path"])])
        if self.ssh_config.get("port", 22) != 22:
            ssh_cmd.extend(["-p", str(self.ssh_config["port"])])

        ssh_cmd.extend([self.connection_string, command])

        result = subprocess.run(ssh_cmd, capture_output=True, text=True)
        return result.stdout, result.stderr, result.returncode

    def get_mock_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        return self.data_dir / "public" / "syft_datasets" / dataset_name

    def get_private_dataset_path(self, dataset_name: str) -> Path:
        """Get the path for a specific dataset."""
        return self.data_dir / "private" / "syft_datasets" / dataset_name
