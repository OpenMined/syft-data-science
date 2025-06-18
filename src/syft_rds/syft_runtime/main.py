import os
import subprocess
import time
from pathlib import Path
from typing import Callable, Protocol

from loguru import logger
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner

from syft_rds.models.models import DockerMount, Job, JobConfig, JobUpdate, RuntimeKind
from syft_rds.syft_runtime.mounts import get_mount_provider

DEFAULT_WORKDIR = "/app"
DEFAULT_OUTPUT_DIR = DEFAULT_WORKDIR + "/output"
SYFT_RDS_BLOCKING_EXECUTION = "SYFT_RDS_BLOCKING_EXECUTION"


class JobOutputHandler(Protocol):
    """Protocol defining the interface for job output handling and display"""

    def on_job_start(self, job_config: JobConfig) -> None:
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

    def on_job_start(self, job_config: JobConfig) -> None:
        self.config = job_config
        self.stdout_file = (job_config.logs_dir / "stdout.log").open("w")
        self.stderr_file = (job_config.logs_dir / "stderr.log").open("w")
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

    def __init__(self, show_stdout: bool = True, show_stderr: bool = True):
        self.show_stdout = show_stdout
        self.show_stderr = show_stderr
        self.console = Console()
        spinner = Spinner("dots")
        self.live = Live(spinner, refresh_per_second=10)

    def on_job_start(self, job_config: JobConfig) -> None:
        self.console.print(
            Panel.fit(
                "\n".join(
                    [
                        "[bold green]Starting job[/]",
                        f"[bold white]Execution:[/] [cyan]{' '.join(job_config.runtime.cmd)} {' '.join(job_config.args)}[/]",
                        f"[bold white]Dataset Dir.:[/]  [cyan]{limit_path_depth(job_config.data_path)}[/]",
                        f"[bold white]Output Dir.:[/]   [cyan]{limit_path_depth(job_config.output_dir)}[/]",
                        f"[bold white]Timeout:[/]  [cyan]{job_config.timeout}s[/]",
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

        if stdout and self.show_stdout:
            self.live.console.print(stdout, end="")
        if stderr and self.show_stderr:
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


class TextUI(JobOutputHandler):
    """Simple text-based implementation of JobOutputHandler using print statements"""

    def __init__(self, show_stdout: bool = True, show_stderr: bool = True):
        self.show_stdout = show_stdout
        self.show_stderr = show_stderr
        self._job_running = False

    def on_job_start(self, config: JobConfig) -> None:
        first_line = "================ Job Configuration ================"
        last_line = "=" * len(first_line)
        print(f"\n{first_line}")
        print(f"Execution:    {' '.join(config.runtime.cmd)} {' '.join(config.args)}")
        print(f"Dataset Dir.: {limit_path_depth(config.data_path)}")
        print(f"Output Dir.:  {limit_path_depth(config.output_dir)}")
        print(f"Timeout:      {config.timeout}s")
        print(f"{last_line}\n")
        print("[STARTING JOB]")
        self._job_running = True

    def on_job_progress(self, stdout: str, stderr: str) -> None:
        if not self._job_running:
            return
        if stdout and self.show_stdout:
            print(stdout, end="")
        if stderr and self.show_stderr:
            print(f"[STDERR] {stderr}", end="")

    def on_job_completion(self, return_code: int) -> None:
        self._job_running = False
        if return_code == 0:
            print("\n[JOB COMPLETED SUCCESSFULLY]\n")
        else:
            print(f"\n[JOB FAILED] Return code: {return_code}\n")

    def __del__(self):
        self._job_running = False


class JobRunner:
    """Base class for running jobs."""

    def __init__(self, handlers: list[JobOutputHandler]):
        self.handlers = handlers

    def run(
        self,
        job_config: JobConfig,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
    ) -> int | None:
        """Run a job"""
        raise NotImplementedError

    def _prepare_job_folders(self, job_config: JobConfig) -> None:
        """Create necessary job folders"""
        job_config.job_path.mkdir(parents=True, exist_ok=True)
        job_config.logs_dir.mkdir(exist_ok=True)
        job_config.output_dir.mkdir(exist_ok=True)
        os.chmod(job_config.output_dir, 0o777)

    def _validate_paths(self, job_config: JobConfig) -> None:
        """Validate that the necessary paths exist and are of the correct type."""
        if not job_config.function_folder.exists():
            raise ValueError(
                f"Function folder {job_config.function_folder} does not exist"
            )
        if not job_config.data_path.exists():
            raise ValueError(f"Dataset folder {job_config.data_path} does not exist")

    def _run_subprocess(
        self,
        cmd: list[str],
        job_config: JobConfig,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
        env: dict | None = None,
    ) -> int | subprocess.Popen:
        job_update = job.get_update_for_in_progress()
        update_job_status(job_update, job)

        for handler in self.handlers:
            handler.on_job_start(job_config)

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        if os.getenv(SYFT_RDS_BLOCKING_EXECUTION, "true").lower() == "true":
            logger.info("Running job in blocking mode")
            return self._run_blocking(process, job, update_job_status)
        else:
            logger.info("Running job in non-blocking mode")
            return process

    def _run_blocking(
        self,
        process: subprocess.Popen,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
    ) -> int:
        # Stream logs
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()
            if stdout_line or stderr_line:
                for handler in self.handlers:
                    handler.on_job_progress(stdout_line, stderr_line)
            if process.poll() is not None:
                logger.debug(
                    f"Process {process.pid} terminated with return code {process.returncode}"
                )
                break
            time.sleep(0.1)

        # Flush remaining output
        for line in process.stdout:
            for handler in self.handlers:
                handler.on_job_progress(line, "")
        for line in process.stderr:
            for handler in self.handlers:
                handler.on_job_progress("", line)

        return_code = process.returncode

        # Update job status
        job_update = job.get_update_for_return_code(return_code)
        update_job_status(job_update, job)

        for handler in self.handlers:
            handler.on_job_completion(process.returncode)

        return process.returncode


class PythonRunner(JobRunner):
    """Handles running jobs directly as Python subprocesses"""

    def run(
        self,
        job_config: JobConfig,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
    ) -> int | subprocess.Popen:
        """Run a job as a Python subprocess"""
        logger.debug(
            f"Running code in '{job_config.function_folder}' on dataset '{job_config.data_path}' with runtime '{job_config.runtime.kind.value}'"
        )

        self._validate_paths(job_config)
        self._prepare_job_folders(job_config)

        cmd = self._prepare_run_command(job_config)

        env = os.environ.copy()
        env.update(job_config.get_env())
        env.update(job_config.extra_env)

        return self._run_subprocess(cmd, job_config, job, update_job_status, env=env)

    def _prepare_run_command(self, job_config: JobConfig) -> list[str]:
        return [
            *job_config.runtime.cmd,
            str(Path(job_config.function_folder) / job_config.args[0]),
            *job_config.args[1:],
        ]


class DockerRunner(JobRunner):
    """Handles running jobs in Docker containers with security constraints"""

    def run(
        self,
        job_config: JobConfig,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
    ) -> int | subprocess.Popen:
        """Run a job in a Docker container"""
        logger.debug(
            f"Running code in '{job_config.function_folder}' on dataset '{job_config.data_path}' with runtime '{job_config.runtime.kind.value}'"
        )

        self._validate_paths(job_config)
        self._prepare_job_folders(job_config)

        self._validate_docker(job_config)

        cmd = self._prepare_run_command(job_config)

        return self._run_subprocess(cmd, job_config, job, update_job_status)

    def _validate_docker(self, job_config: JobConfig) -> None:
        """Validate Docker is available and the required image exists."""
        self._check_docker_daemon()
        self._check_or_build_image(job_config)

    def _check_docker_daemon(self) -> None:
        """Check if the Docker daemon is running."""
        try:
            subprocess.run(["docker", "info"], check=True, capture_output=True)
            logger.debug("Docker daemon is available")
        except FileNotFoundError:
            raise RuntimeError("Docker not installed or not in PATH.")
        except subprocess.CalledProcessError:
            raise RuntimeError("Docker daemon is not running.")

    def _get_image_name(self, job_config: JobConfig) -> str:
        """Get the Docker image name from the config or use the default."""
        runtime_config = job_config.runtime.config
        if not runtime_config.image_name:
            return job_config.runtime.name
        return runtime_config.image_name

    def _check_or_build_image(self, job_config: JobConfig) -> None:
        """Check if a required Docker image exists."""
        image_name = self._get_image_name(job_config)
        result = subprocess.run(
            ["docker", "image", "inspect", image_name],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode == 0:
            logger.debug(f"Docker image '{image_name}' already exists.")
            return

        self._build_docker_image(job_config)

    def _build_docker_image(self, job_config: JobConfig) -> None:
        """Build the Docker image."""
        image_name = self._get_image_name(job_config)
        dockerfile_path = (
            Path(job_config.runtime.config.dockerfile).expanduser().resolve()
        )
        if not dockerfile_path.exists():
            raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}")
        logger.info(f"Docker image '{image_name}' not found. Building it now...")
        build_context = dockerfile_path.parent
        try:
            build_cmd = [
                "docker",
                "build",
                "-t",
                image_name,
                "-f",
                str(dockerfile_path),
                str(build_context),
            ]
            logger.debug(f"Running docker build command: {' '.join(build_cmd)}")
            process = subprocess.run(
                build_cmd, capture_output=True, check=True, text=True
            )

            logger.debug(process.stdout)
            logger.info(f"Successfully built Docker image '{image_name}'.")

        except FileNotFoundError:
            raise RuntimeError("Docker not installed or not in PATH.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Docker build failed. stderr: {e.stderr}")
            raise RuntimeError(f"Failed to build Docker image '{image_name}'.")
        except Exception as e:
            raise RuntimeError(f"An error occurred during Docker image build: {e}")

    def _get_extra_mounts(self, job_config: JobConfig) -> list[DockerMount]:
        """Get extra mounts for a job"""
        docker_runtime_config = job_config.runtime.config
        if docker_runtime_config.app_name is None:
            return []
        mount_provider = get_mount_provider(docker_runtime_config.app_name)
        if mount_provider:
            return mount_provider.get_mounts(job_config)
        return []

    def _prepare_run_command(self, job_config: JobConfig) -> list[str]:
        """Build the Docker run command with security constraints"""
        image_name = self._get_image_name(job_config)
        docker_mounts = [
            "-v",
            f"{Path(job_config.function_folder).absolute()}:{DEFAULT_WORKDIR}/code:ro",
            "-v",
            f"{Path(job_config.data_path).absolute()}:{DEFAULT_WORKDIR}/data:ro",
            "-v",
            f"{job_config.output_dir.absolute()}:{DEFAULT_OUTPUT_DIR}:rw",
        ]

        extra_mounts = self._get_extra_mounts(job_config)
        if extra_mounts:
            for mount in extra_mounts:
                docker_mounts.extend(
                    [
                        "-v",
                        f"{mount.source.resolve()}:{mount.target}:{mount.mode}",
                    ]
                )

        interpreter = " ".join(job_config.runtime.cmd)
        interpreter_str = f'"{interpreter}"' if " " in interpreter else interpreter

        limits = [
            # Security constraints
            "--cap-drop",
            "ALL",  # Drop all capabilities
            "--network",
            "none",  # Disable networking
            # "--read-only",  # Read-only root filesystem - TODO: re-enable this
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

        docker_run_cmd = [
            "docker",
            "run",
            "--rm",  # Remove container after completion
            *limits,
            # Environment variables
            "-e",
            f"TIMEOUT={job_config.timeout}",
            "-e",
            f"DATA_DIR={job_config.data_mount_dir}",
            "-e",
            f"OUTPUT_DIR={DEFAULT_OUTPUT_DIR}",
            "-e",
            f"INTERPRETER={interpreter_str}",
            "-e",
            f"INPUT_FILE='{DEFAULT_WORKDIR}/code/{job_config.args[0]}'",
            *job_config.get_extra_env_as_docker_args(),
            *docker_mounts,
            "--workdir",
            DEFAULT_WORKDIR,
            image_name,
            f"{DEFAULT_WORKDIR}/code/{job_config.args[0]}",
            *job_config.args[1:],
        ]
        logger.debug(f"Docker run command: {docker_run_cmd}")
        return docker_run_cmd


class SyftRunner:
    """A factory class to select and run the appropriate job runner."""

    def __init__(self, handlers: list[JobOutputHandler]):
        self.handlers = handlers
        self._runners = {
            RuntimeKind.PYTHON: PythonRunner(handlers),
            RuntimeKind.DOCKER: DockerRunner(handlers),
        }

    def run(
        self,
        job_config: JobConfig,
        job: Job,
        update_job_status: Callable[[JobUpdate, Job], Job],
    ) -> int | None:
        """Selects the appropriate runner based on runtime kind and runs the job."""
        runner = self._runners.get(job_config.runtime.kind)
        if not runner:
            raise NotImplementedError(
                f"Unsupported runtime kind: {job_config.runtime.kind}"
            )
        return runner.run(job_config, job, update_job_status)
