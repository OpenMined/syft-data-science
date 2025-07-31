import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

from loguru import logger
from syft_runtimes.runtimes import RemoteConnection, RemoteRuntime
from syft_runtimes.runtimes import RuntimeType, RuntimeExecutionResult
from syft_runtimes.models import BaseRuntimeConfig


class SSHConnection(RemoteConnection):
    """SSH connection implementation."""

    def __init__(
        self,
        host: str,
        user: str,
        port: int = 22,
        ssh_key_path: Optional[Path] = None,
        password: Optional[str] = None,
        ssh_options: Optional[Dict[str, str]] = None,
    ):
        self.host = host
        self.user = user
        self.port = port
        self.ssh_key_path = ssh_key_path
        self.password = password
        self.ssh_options = ssh_options or {}
        self.connection_string = f"{user}@{host}"

    def _build_ssh_command(self, remote_command: str) -> list[str]:
        """Build SSH command with all options."""
        cmd = ["ssh"]

        # Add SSH key if provided
        if self.ssh_key_path:
            cmd.extend(["-i", str(self.ssh_key_path)])

        # Add port if not default
        if self.port != 22:
            cmd.extend(["-p", str(self.port)])

        # Add additional SSH options
        for option, value in self.ssh_options.items():
            cmd.extend(["-o", f"{option}={value}"])

        # Add connection and command
        cmd.extend([self.connection_string, remote_command])

        return cmd

    def execute(self, command: str, **kwargs) -> RuntimeExecutionResult:
        """Execute command via SSH."""
        ssh_cmd = self._build_ssh_command(command)

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=kwargs.get("timeout", 300),
            )

            return RuntimeExecutionResult(
                success=result.returncode == 0,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.returncode,
                metadata={"ssh_command": " ".join(ssh_cmd)},
            )
        except subprocess.TimeoutExpired as e:
            return RuntimeExecutionResult(
                success=False, error_message=f"SSH command timed out: {e}", exit_code=-1
            )
        except Exception as e:
            return RuntimeExecutionResult(
                success=False, error_message=f"SSH command failed: {e}", exit_code=-1
            )

    def upload_file(self, local_path: Path, remote_path: Path) -> bool:
        """Upload file via SCP."""
        scp_cmd = ["scp"]

        if self.ssh_key_path:
            scp_cmd.extend(["-i", str(self.ssh_key_path)])
        if self.port != 22:
            scp_cmd.extend(["-P", str(self.port)])

        scp_cmd.extend([str(local_path), f"{self.connection_string}:{remote_path}"])

        try:
            result = subprocess.run(scp_cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"SCP upload failed: {e}")
            return False

    def download_file(self, remote_path: Path, local_path: Path) -> bool:
        """Download file via SCP."""
        scp_cmd = ["scp"]

        if self.ssh_key_path:
            scp_cmd.extend(["-i", str(self.ssh_key_path)])
        if self.port != 22:
            scp_cmd.extend(["-P", str(self.port)])

        scp_cmd.extend([f"{self.connection_string}:{remote_path}", str(local_path)])

        try:
            result = subprocess.run(scp_cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"SCP download failed: {e}")
            return False

    def close(self) -> None:
        """Close SSH connection (no persistent connection in this implementation)."""
        pass


class SSHRuntime(RemoteRuntime):
    """SSH-based remote runtime implementation."""

    def __init__(
        self,
        runtime_name: str,
        ssh_config: Dict[str, Any],
        remote_base_dir: Path,
        config: Optional[BaseRuntimeConfig] = None,
    ):
        if config is None:
            config = BaseRuntimeConfig(
                config_path=remote_base_dir
                / "private"
                / "syft_runtimes"
                / runtime_name
                / "config.yaml"
            )

        # Create SSH connection
        connection = SSHConnection(
            host=ssh_config["host"],
            user=ssh_config["user"],
            port=ssh_config.get("port", 22),
            ssh_key_path=ssh_config.get("ssh_key_path"),
            password=ssh_config.get("password"),
            ssh_options=ssh_config.get("ssh_options", {}),
        )

        super().__init__(runtime_name, config, connection, remote_base_dir)

    def _get_runtime_type(self) -> RuntimeType:
        return RuntimeType.SSH
