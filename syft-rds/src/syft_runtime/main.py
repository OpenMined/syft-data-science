import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Protocol, Tuple

from pydantic import BaseModel, Field
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner

DEFAULT_OUTPUT_DIR = "/output"


class CodeRuntime(BaseModel):
    cmd: list[str]
    image_name: str | None = None
    mount_dir: Path | None = None

    @classmethod
    def default(cls):
        return cls(
            cmd=["python"],
        )


class JobConfig(BaseModel):
    """Configuration for a job run"""

    function_folder: Path
    args: list[str]
    data_path: Path
    runtime: CodeRuntime
    job_folder: Optional[Path] = Field(
        default_factory=lambda: Path("jobs") / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    timeout: int = 60
    data_mount_dir: str = "/data"
    use_docker: bool = True
    extra_env: dict[str, str] = {}

    @property
    def job_path(self) -> Path:
        """Derived path for job folder"""
        return Path(self.job_folder)

    @property
    def logs_dir(self) -> Path:
        """Derived path for logs directory"""
        return self.job_path / "logs"

    @property
    def output_dir(self) -> Path:
        """Derived path for output directory"""
        return self.job_path / "output"

    def get_env(self) -> dict[str, str]:
        return self.extra_env | self._base_env

    def get_env_as_docker_args(self) -> list[str]:
        return [f"-e {k}={v}" for k, v in self.get_env().items()]

    def get_extra_env_as_docker_args(self) -> list[str]:
        return [f"-e {k}={v}" for k, v in self.extra_env.items()]

    @property
    def _base_env(self) -> dict[str, str]:
        interpreter = " ".join(self.runtime.cmd)
        # interpreter_str = f"'{interpreter}'" if " " in interpreter else interpreter
        return {
            "OUTPUT_DIR": str(self.output_dir.absolute()),
            "DATA_DIR": str(self.data_path.absolute()),
            "TIMEOUT": str(self.timeout),
            "INPUT_FILE": str(self.function_folder / self.args[0]),
            "INTERPRETER": interpreter,
        }


class JobOutputHandler(Protocol):
    """Protocol defining the interface for job output handling and display"""

    def on_job_start(self, config: JobConfig) -> None:
        """Display job configuration"""
        pass

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        """Display job progress"""
        pass

    def on_job_completion(self, return_code: int) -> None:
        """Display job completion status"""
        pass


class FileOutputHandler(JobOutputHandler):
    """Handles writing job output to log files"""

    def __init__(self):
        pass

    def on_job_start(self, config: JobConfig) -> None:
        self.config = config
        self.stdout_file = (config.logs_dir / "stdout.log").open("w")
        self.stderr_file = (config.logs_dir / "stderr.log").open("w")
        self.on_job_progress(stdout="Starting job...\n", stderr="Starting job...\n")

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        if stdout:
            self.stdout_file.write(stdout)
            self.stdout_file.flush()
        if stderr:
            self.stderr_file.write(stderr)
            self.stderr_file.flush()

    def on_job_completion(self, return_code: int) -> None:
        self.on_job_progress(
            stdout=f"Job completed with return code {return_code}\n",
            stderr=f"Job completed with return code {return_code}\n",
        )
        self.close()

    def close(self) -> None:
        self.stdout_file.close()
        self.stderr_file.close()


# Helper function to limit path depth
def limit_path_depth(path: Path, max_depth: int = 4) -> str:
    parts = path.parts
    if len(parts) <= max_depth:
        return str(path)
    return str(Path("...") / Path(*parts[-max_depth:]))


class RichConsoleUI(JobOutputHandler):
    """Rich console implementation of JobOutputHandler"""

    def __init__(self):
        self.console = Console()
        spinner = Spinner("dots")
        self.live = Live(spinner, refresh_per_second=10)

    def on_job_start(self, config: JobConfig) -> None:
        self.console.print(
            Panel.fit(
                "\n".join(
                    [
                        "[bold green]Starting job[/]",
                        f"[bold white]Execution:[/] [cyan]{' '.join(config.runtime.cmd)} {' '.join(config.args)}[/]",
                        f"[bold white]Dataset Dir.:[/]  [cyan]{limit_path_depth(config.data_path)}[/]",
                        f"[bold white]Output Dir.:[/]   [cyan]{limit_path_depth(config.output_dir)}[/]",
                        f"[bold white]Timeout:[/]  [cyan]{config.timeout}s[/]",
                    ]
                ),
                title="[bold]Job Configuration",
                border_style="cyan",
            )
        )
        try:
            self.live.start()
            self.live.console.print("[bold cyan]Running job...[/]")
        except Exception as e:
            self.console.print(f"[red]Error starting live: {e}[/]")

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        # Update UI display
        if not self.live:
            return

        if stdout:
            self.live.console.print(stdout, end="")
        if stderr:
            self.live.console.print(f"[red]{stderr}[/]", end="")

    def on_job_completion(self, return_code: int) -> None:
        # Update UI display
        if self.live:
            self.live.stop()

        if return_code == 0:
            self.console.print("\n[bold green]Job completed successfully![/]")
        else:
            self.console.print(
                f"\n[bold red]Job failed with return code {return_code}[/]"
            )

    def __del__(self):
        self.live.stop()


class DockerRunner:
    """Handles running jobs in Docker containers with security constraints"""

    def __init__(self, handlers: list[JobOutputHandler]):
        self.handlers = handlers

    def prepare_job_folders(self, config: JobConfig) -> None:
        """Create necessary job folders"""
        config.job_path.mkdir(parents=True, exist_ok=True)
        config.logs_dir.mkdir(exist_ok=True)
        config.output_dir.mkdir(exist_ok=True)

    def validate_paths(self, config: JobConfig) -> None:
        """Validate input paths exist"""
        if not config.function_folder.exists():
            raise ValueError(f"Function folder {config.function_folder} does not exist")
        if not config.data_path.exists():
            raise ValueError(f"Dataset folder {config.data_path} does not exist")

    def build_docker_command(self, config: JobConfig) -> list[str]:
        """Build the Docker run command with security constraints"""

        if not config.use_docker:
            # For direct Python execution, build a command that runs Python directly
            # Assuming the first arg is the Python script to run
            return [
                *config.runtime.cmd,
                str(Path(config.function_folder) / config.args[0]),
                *config.args[1:],
            ]
        config.output_dir.absolute().mkdir(parents=True, exist_ok=True)
        docker_mounts = [
            "-v",
            f"{Path(config.function_folder).absolute()}:/code:ro",
            "-v",
            f"{Path(config.data_path).absolute()}:{config.data_mount_dir}:ro",
            "-v",
            f"{config.output_dir.absolute()}:{DEFAULT_OUTPUT_DIR}:rw",
        ]

        if config.runtime.mount_dir:
            docker_mounts.extend(
                [
                    "-v",
                    f"{config.runtime.mount_dir.absolute()}:{config.runtime.mount_dir.absolute()}:ro",
                ]
            )

        limits = [
            # Security constraints
            "--cap-drop",
            "ALL",  # Drop all capabilities
            "--network",
            "none",  # Disable networking
            "--read-only",  # Read-only root filesystem
            "--tmpfs",
            "/tmp:size=16m,noexec,nosuid,nodev",  # Secure temp directory
            # Resource limits
            "--memory",
            "1G",
            "--cpus",
            "1",
            "--pids-limit",
            "100",
            "--ulimit",
            "nproc=4096:4096",
            "--ulimit",
            "nofile=50:50",
            "--ulimit",
            "fsize=10000000:10000000",  # ~10MB file size limit
        ]
        interpreter = " ".join(config.runtime.cmd)
        interpreter_str = f'"{interpreter}"' if " " in interpreter else interpreter
        return [
            "docker",
            "run",
            "--rm",  # Remove container after completion
            *limits,
            # Environment variables
            "-e",
            f"TIMEOUT={config.timeout}",
            "-e",
            f"DATA_DIR={config.data_mount_dir}",
            "-e",
            f"OUTPUT_DIR={DEFAULT_OUTPUT_DIR}",
            "-e",
            f"INTERPRETER={interpreter_str}",
            "-e",
            f"INPUT_FILE='{config.function_folder / config.args[0]}'",
            *config.get_extra_env_as_docker_args(),
            *docker_mounts,
            "syft_python_runtime",
            *config.args,
        ]

    def validate_docker(self, config: JobConfig) -> bool:
        """Validate Docker image availability"""
        if not config.use_docker:
            return True  # Skip Docker validation when not using Docker

        image_name = "syft_python_runtime"
        # Check Docker daemon availability
        subprocess.run(["docker", "info"], check=True, capture_output=True)

        # Check if required image exists
        try:
            result = subprocess.run(
                ["docker", "image", "inspect", image_name],
                capture_output=True,
                check=False,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(f"\n\n{result.stderr}")
            return True
        except FileNotFoundError:
            raise RuntimeError("Docker not installed or not in PATH")

    def run(self, config: JobConfig) -> Tuple[Path, int | None]:
        """Run a job in a Docker container or directly as Python"""
        # Check Docker availability first if using Docker
        if not self.validate_docker(config):
            return -1

        self.validate_paths(config)
        self.prepare_job_folders(config)

        for handler in self.handlers:
            handler.on_job_start(config)

        cmd = self.build_docker_command(config)

        # Set up environment variables for direct Python execution
        env = None
        if not config.use_docker:
            env = os.environ.copy()
            env.update(config.get_env())
            env.update(config.extra_env)

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        # Stream output
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()

            for handler in self.handlers:
                handler.on_job_progress(stdout_line, stderr_line)

            if not stdout_line and not stderr_line and process.poll() is not None:
                break

            if not stdout_line and not stderr_line:
                time.sleep(0.5)

        process.wait()
        for handler in self.handlers:
            handler.on_job_completion(process.returncode)

        return process.returncode
