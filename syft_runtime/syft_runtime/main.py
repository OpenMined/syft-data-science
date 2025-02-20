import enum
import subprocess
from pathlib import Path
from datetime import datetime
import time
from typing import Callable, Protocol, Tuple, Optional, TextIO

import typer
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.spinner import Spinner
from pydantic import BaseModel, Field

# Add imports for Jupyter UI
try:
    from IPython.display import display, HTML, clear_output
    from ipywidgets import HTML as HTMLWidget
    from ipywidgets import Layout, Output, VBox, HBox, Label, IntProgress

    JUPYTER_AVAILABLE = True
except ImportError:
    JUPYTER_AVAILABLE = False

DEFAULT_OUTPUT_DIR = "/output"

console = Console()
app = typer.Typer()


class CodeRuntime(str, enum.Enum):
    python = "python"
    bash = "bash"
    sql = "sql"


class JobConfig(BaseModel):
    """Configuration for a job run"""

    function_folder: Path
    args: list[str]
    data_path: Path
    runtime: CodeRuntime = CodeRuntime.python
    job_folder: Optional[Path] = Field(
        default_factory=lambda: Path("jobs") / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    timeout: int = 1
    data_mount_dir: str = "/data"

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
        self.open_files(config.logs_dir)
        self.on_job_progress(stdout="Starting job...\n", stderr="Starting job...\n")

    def open_files(self, logs_dir: Path) -> None:
        self.stdout_file = (logs_dir / "stdout.log").open("w")
        self.stderr_file = (logs_dir / "stderr.log").open("w")

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        """Write output to log files"""
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
        """Close log files"""
        self.stdout_file.close()
        self.stderr_file.close()


class RichConsoleUI(JobOutputHandler):
    """Rich console implementation of JobOutputHandler"""

    def __init__(self):
        self.console = Console()
        self.live: Optional[Live] = None

    def on_job_start(self, config: JobConfig) -> None:
        if config.data_path.is_file():
            data_mount_display = str(
                Path(config.data_mount_dir) / config.data_path.name
            )
        else:
            data_mount_display = config.data_mount_dir

        self.console.print(
            Panel.fit(
                "\n".join(
                    [
                        "[bold green]Starting job[/]",
                        f"[bold white]Function:[/] [cyan]{config.function_folder}[/] → [dim]/code[/]",
                        f"[bold white]Args:[/] [cyan]{' '.join(config.args)}[/] → [dim]/code[/]",
                        f"[bold white]Dataset:[/]  [cyan]{config.data_path}[/] → [dim]{data_mount_display}[/]",
                        f"[bold white]Output:[/]   [cyan]{config.output_dir}[/] → [dim]{DEFAULT_OUTPUT_DIR}[/]",
                        f"[bold white]Timeout:[/]  [cyan]{config.timeout}s[/]",
                    ]
                ),
                title="[bold]Job Configuration",
                border_style="cyan",
            )
        )

        spinner = Spinner("dots")
        self.live = Live(spinner, refresh_per_second=10)
        self.live.start()
        self.live.console.print("[bold cyan]Running job...[/]")

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
            self.console.print(f"\n[bold green]Job completed successfully![/]")
        else:
            self.console.print(
                f"\n[bold red]Job failed with return code {return_code}[/]"
            )


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
            raise typer.Abort(
                f"Function folder {config.function_folder} does not exist"
            )
        if not config.data_path.exists():
            raise typer.Abort(f"Dataset folder {config.data_path} does not exist")

    def build_docker_command(self, config: JobConfig) -> list[str]:
        """Build the Docker run command with security constraints"""
        docker_mounts = [
            "-v",
            f"{Path(config.function_folder).absolute()}:/code:ro",
            "-v",
            f"{Path(config.data_path).absolute()}:{config.data_mount_dir}:ro",
            "-v",
            f"{config.output_dir.absolute()}:{DEFAULT_OUTPUT_DIR}:rw",
        ]

        return [
            "docker",
            "run",
            "--rm",  # Remove container after completion
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
            "nproc=100:100",
            "--ulimit",
            "nofile=50:50",
            "--ulimit",
            "fsize=10000000:10000000",  # ~10MB file size limit
            # Environment variables
            "-e",
            f"TIMEOUT={config.timeout}",
            "-e",
            f"DATA_DIR={config.data_mount_dir}",
            "-e",
            f"OUTPUT_DIR={DEFAULT_OUTPUT_DIR}",
            *docker_mounts,
            f"syft_{config.runtime.value}_runtime",
            *config.args,
        ]

    def run(self, config: JobConfig) -> Tuple[Path, int | None]:
        """Run a job in a Docker container"""
        self.validate_paths(config)
        self.prepare_job_folders(config)

        for handler in self.handlers:
            handler.on_job_start(config)

        cmd = self.build_docker_command(config)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
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

        return config.job_path, process.returncode


@app.command()
def main(name: str):
    print(f"Hello {name}")


@app.command()
def run(
    function_folder: Path,
    args: list[str],
    data_path: Path,
    runtime: CodeRuntime = CodeRuntime.python,
    job_folder: Path | None = None,
    timeout: int = 1,
    data_mount_dir: str = "/data",
    ui: str = "rich",  # Add UI selection parameter
    output_handle: Optional[Output] = None,  # Add output handle parameter
) -> Tuple[Path, int | None]:
    """
    Run a Docker container with strict security constraints

    Args:
        function_folder: Path to the function code directory
        data_path: Path to the dataset directory
        job_folder: Path to store job outputs (default: auto-generated)
        timeout: Maximum execution time in seconds (default: 1 second)
        data_mount_path: Mount path for data inside container (default: /data)
        ui: UI type to use ("rich" or "jupyter", default: "rich")
        output_handle: Optional output handle for Jupyter UI (default: None)

    Returns:
        Tuple containing:
        - Path to the job output directory
        - Return code from the Docker container (None if job was interrupted)
    """
    config = JobConfig(
        function_folder=function_folder,
        args=args,
        data_path=data_path,
        runtime=runtime,
        job_folder=job_folder,
        timeout=timeout,
        data_mount_dir=data_mount_dir,
    )

    # Select UI based on parameter and environment
    if ui == "jupyter" and JUPYTER_AVAILABLE:
        runner = DockerRunner(ui=JupyterUI(handle=output_handle))
    else:
        runner = DockerRunner(ui=RichConsoleUI())

    return runner.run(config)


# if __name__ == "__main__":
#     job_path, rc = run_docker_job("ds/function1", "do/dataset1", timeout=20)
#     print(f"Job path: {job_path}")
#     print(f"Return code: {rc}")
