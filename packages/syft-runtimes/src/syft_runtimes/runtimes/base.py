from abc import ABC, abstractmethod
import subprocess
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel
from loguru import logger

from syft_core import Client as SyftBoxClient

from syft_runtimes.models import BaseRuntimeConfig
from syft_runtimes.consts import RUNTIME_SUBDIRECTORIES


class RuntimeType(str, Enum):
    """Types of runtime execution environments."""

    LOCAL = "local"
    DOCKER_SWARM = "docker"
    KUBERNETES = "kubernetes"
    # some other possible runtime types
    # AWS_BATCH = "aws_batch"
    # GCP_CLOUD_RUN = "gcp_cloud_run"
    # AZURE_CONTAINER = "azure_container"
    # SLURM = "slurm"
    # PBS = "pbs"
    # SGE = "sge"


class RuntimeExecutionResult(BaseModel):
    """Result of a runtime operation."""

    success: bool
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = {}


class Runtime(ABC):
    """Abstract base class for all runtime execution environments."""

    def __init__(self, runtime_name: str, config: BaseRuntimeConfig):
        self.runtime_name = runtime_name
        self.config = config
        self.runtime_type = self._get_runtime_type()

    @abstractmethod
    def _get_runtime_type(self) -> RuntimeType:
        """Return the runtime type."""
        pass

    @abstractmethod
    def init_runtime_dir(self) -> Path:
        """Initialize the runtime directory structure."""
        pass

    @abstractmethod
    def ensure_directory_exists(self, path: Path) -> bool:
        """Ensure a directory exists in the runtime environment."""
        pass

    @abstractmethod
    def execute_command(self, command: str, **kwargs) -> RuntimeExecutionResult:
        """Execute a command in the runtime environment."""
        pass

    @abstractmethod
    def cleanup(self) -> bool:
        """Clean up runtime resources."""
        pass

    @property
    def is_local(self) -> bool:
        """Check if this is a local runtime."""
        return self.runtime_type == RuntimeType.LOCAL

    @property
    def is_remote(self) -> bool:
        """Check if this is a remote runtime."""
        return not self.is_local


class LocalRuntime(Runtime):
    """Local runtime implementation (current FolderBasedRuntime functionality)."""

    def __init__(self, syftbox_client: SyftBoxClient, runtime_name: str):
        self.syftbox_client = syftbox_client
        self.syft_runtimes_dir = (
            self.syftbox_client.workspace.data_dir
            / "private"
            / self.syftbox_client.email
            / "syft_runtimes"
        )
        self.runtime_dir = self.syft_runtimes_dir / runtime_name

        config = BaseRuntimeConfig(config_path=self.runtime_dir / "config.yaml")
        super().__init__(runtime_name, config)

    def _get_runtime_type(self) -> RuntimeType:
        return RuntimeType.LOCAL

    def init_runtime_dir(self) -> Path:
        """Initialize the runtime directory structure locally."""
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

        # Create standard directories
        for dir_name in RUNTIME_SUBDIRECTORIES:
            (self.runtime_dir / dir_name).mkdir(exist_ok=True)

        # Save config
        self.config.save_to_yaml()

        logger.debug(f"Local runtime initialized at: {self.runtime_dir}")
        return self.runtime_dir

    def ensure_directory_exists(self, path: Path) -> bool:
        """Create directory locally."""
        try:
            path.mkdir(parents=True, exist_ok=True)
            return True
        except OSError as e:
            logger.error(f"Failed to create directory {path}: {e}")
            return False

    def execute_command(self, command: str, **kwargs) -> RuntimeExecutionResult:
        """Execute command locally."""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=kwargs.get("timeout", 300),
            )

            return RuntimeExecutionResult(
                success=result.returncode == 0,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.returncode,
            )
        except subprocess.TimeoutExpired as e:
            return RuntimeExecutionResult(
                success=False, error_message=f"Command timed out: {e}", exit_code=-1
            )
        except Exception as e:
            return RuntimeExecutionResult(
                success=False, error_message=f"Command failed: {e}", exit_code=-1
            )

    def cleanup(self) -> bool:
        """Clean up local runtime resources."""
        # For local runtime, cleanup might involve removing temp files
        # or performing other housekeeping tasks
        self.runtime_dir.rmdir()
        return True


class RemoteConnection:
    """Abstract base for remote connections."""

    @abstractmethod
    def execute(self, command: str, **kwargs) -> RuntimeExecutionResult:
        """Execute command on remote system."""
        pass

    @abstractmethod
    def upload_file(self, local_path: Path, remote_path: Path) -> bool:
        """Upload file to remote system."""
        pass

    @abstractmethod
    def download_file(self, remote_path: Path, local_path: Path) -> bool:
        """Download file from remote system."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        pass


class RemoteRuntime(Runtime):
    """Base class for all remote runtime implementations."""

    def __init__(
        self,
        runtime_name: str,
        config: BaseRuntimeConfig,
        connection: RemoteConnection,
        remote_base_dir: Path,
    ):
        super().__init__(runtime_name, config)
        self.connection = connection
        self.remote_base_dir = remote_base_dir
        self.runtime_dir = remote_base_dir / "private" / "syft_runtimes" / runtime_name

    def init_runtime_dir(self) -> Path:
        """Initialize runtime directory on remote system."""
        # Create main runtime directory
        if not self.ensure_directory_exists(self.runtime_dir):
            raise RuntimeError(
                f"Failed to create runtime directory: {self.runtime_dir}"
            )

        # Create standard subdirectories
        for dir_name in RUNTIME_SUBDIRECTORIES:
            dir_path = self.runtime_dir / dir_name
            if not self.ensure_directory_exists(dir_path):
                raise RuntimeError(f"Failed to create directory: {dir_path}")

        # Create and upload config file
        self._upload_config_file()

        logger.debug(f"Remote runtime initialized at: {self.runtime_dir}")
        return self.runtime_dir

    def ensure_directory_exists(self, path: Path) -> bool:
        """Create directory on remote system."""
        command = f"mkdir -p {path}"
        result = self.connection.execute(command)
        logger.info(result)
        return result.success

    def execute_command(self, command: str, **kwargs) -> RuntimeExecutionResult:
        """Execute command on remote system."""
        return self.connection.execute(command, **kwargs)

    def cleanup(self) -> bool:
        """Clean up remote runtime resources."""
        try:
            self.connection.close()
            return True
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False
