import subprocess
from pathlib import Path
from datetime import datetime
from typing import Tuple
import sys

import typer
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.spinner import Spinner

DEFAULT_OUTPUT_DIR = "/output"

console = Console()
app = typer.Typer()


@app.command()
def main(name: str):
    print(f"Hello {name}")


@app.command()
def run(
    function_folder: Path,
    data_path: Path,
    job_folder: Path | None = None,
    timeout: int = 1,
    data_mount_dir: str = "/data",
) -> Tuple[Path, int | None]:
    """
    Run a Docker container with strict security constraints

    Args:
        function_folder: Path to the function code directory
        data_path: Path to the dataset directory
        job_folder: Path to store job outputs (default: auto-generated)
        timeout: Maximum execution time in seconds (default: 1 second)
        data_mount_path: Mount path for data inside container (default: /data)

    Returns:
        Tuple containing:
        - Path to the job output directory
        - Return code from the Docker container (None if job was interrupted)
    """

    # Validate input paths exist
    if not function_folder.exists():
        raise typer.Abort(f"Function folder {function_folder} does not exist")
    if not data_path.exists():
        raise typer.Abort(f"Dataset folder {data_path} does not exist")

    # Create job output directory
    if job_folder is None:
        job_folder = Path("jobs") / datetime.now().strftime("%Y%m%d_%H%M%S")
    job_path = Path(job_folder)
    job_path.mkdir(parents=True, exist_ok=True)
    logs_dir = job_path / "logs"
    logs_dir.mkdir(exist_ok=True)

    output_dir = job_path / "output"
    output_dir.mkdir(exist_ok=True)

    # Handle single file data paths
    if data_path.is_file():
        data_mount_dir = str(Path(data_mount_dir) / data_path.name)
        print(f"Data mount path: {data_mount_dir}")

    # Display job configuration
    console.print(
        Panel.fit(
            "\n".join(
                [
                    "[bold green]Starting job[/]",
                    f"[bold white]Function:[/] [cyan]{function_folder}[/] → [dim]/code[/]",
                    f"[bold white]Dataset:[/]  [cyan]{data_path}[/] → [dim]{data_mount_dir}[/]",
                    f"[bold white]Output:[/]   [cyan]{output_dir}[/] → [dim]{DEFAULT_OUTPUT_DIR}[/]",
                    f"[bold white]Timeout:[/]  [cyan]{timeout}s[/]",
                ]
            ),
            title="[bold]Job Configuration",
            border_style="cyan",
        )
    )

    # Configure Docker volume mounts
    docker_mounts = [
        "-v",
        f"{Path(function_folder).absolute()}:/code:ro",
        "-v",
        f"{Path(data_path).absolute()}:{data_mount_dir}:ro",
        "-v",
        f"{output_dir.absolute()}:{DEFAULT_OUTPUT_DIR}:rw",
    ]

    # Build Docker run command with security constraints
    cmd = [
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
        f"TIMEOUT={timeout}",
        "-e",
        f"DATA_PATH={data_mount_dir}",
        "-e",
        f"OUTPUT_DIR={DEFAULT_OUTPUT_DIR}",
        *docker_mounts,
        "pythonrunner",
    ]

    # Run Docker container and capture output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Display progress spinner and stream logs
    spinner = Spinner("dots")
    with Live(spinner, refresh_per_second=10) as live:
        live.console.print("[bold cyan]Running job...[/]")

        # Stream and save logs
        with (
            (logs_dir / "stdout.log").open("w") as stdout_file,
            (logs_dir / "stderr.log").open("w") as stderr_file,
        ):
            while True:
                stdout_line = process.stdout.readline()
                stderr_line = process.stderr.readline()

                if stdout_line:
                    live.console.print(stdout_line, end="")
                    stdout_file.write(stdout_line)
                    stdout_file.flush()

                if stderr_line:
                    live.console.print(f"[red]{stderr_line}[/]", end="")
                    stderr_file.write(stderr_line)
                    stderr_file.flush()

                if not stdout_line and not stderr_line and process.poll() is not None:
                    break

            # Wait for process to complete
            process.wait()
            live.stop()

            # Display completion status
            if process.returncode == 0:
                console.print(f"\n[bold green]Job completed successfully![/]")
            else:
                console.print(
                    f"\n[bold red]Job failed with return code {process.returncode}[/]"
                )

            console.print(f"[dim]Job output saved to: {job_path}[/]")
            typer.Exit(process.returncode)


# if __name__ == "__main__":
#     job_path, rc = run_docker_job("ds/function1", "do/dataset1", timeout=20)
#     print(f"Job path: {job_path}")
#     print(f"Return code: {rc}")
