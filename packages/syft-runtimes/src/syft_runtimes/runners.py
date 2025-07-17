import os
import subprocess
import time
import threading
import json
import shutil
from abc import abstractmethod
from pathlib import Path
from typing import Callable, Optional, Type
from uuid import uuid4

from loguru import logger
from syft_core import Client as SyftBoxClient

from syft_runtimes.models import (
    BaseRuntimeConfig,
    DockerMount,
    JobConfig,
    RuntimeKind,
    JobStatus,
    JobErrorKind,
    JobStatusUpdate,
    JobResults,
)
from syft_runtimes.mounts import get_mount_provider
from syft_runtimes.output_handler import JobOutputHandler


DEFAULT_WORKDIR = "/app"
DEFAULT_OUTPUT_DIR = DEFAULT_WORKDIR + "/output"


class SyftRuntime:
    """Base class for running jobs."""

    def __init__(
        self,
        handlers: list[JobOutputHandler],
        update_job_status_callback: Optional[Callable[[JobStatusUpdate], None]] = None,
    ):
        self.handlers = handlers
        self.update_job_status_callback = update_job_status_callback

    def run(
        self,
        job_config: JobConfig,
    ) -> tuple[int, str | None] | subprocess.Popen:
        """Run a job
        Returns:
            tuple[int, str | None]: (blocking mode) The return code and error message
                if the job failed, otherwise None.
            subprocess.Popen: (non-blocking mode) The process object.
        """
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
        env: dict | None = None,
        blocking: bool = True,
    ) -> tuple[int, str | None] | subprocess.Popen:
        """
        Returns:
            tuple[int, str | None]: (blocking mode) The return code and error message
                if the job failed, otherwise None.
            subprocess.Popen: (non-blocking mode) The process object.
        """
        if self.update_job_status_callback:
            self.update_job_status_callback(
                JobStatusUpdate(
                    status=JobStatus.job_in_progress,
                    error=JobErrorKind.no_error,
                    error_message=None,
                )
            )

        for handler in self.handlers:
            handler.on_job_start(job_config)

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        if blocking:
            logger.info("Running job in blocking mode")
            return self._run_blocking(process)
        else:
            logger.info("Running job in non-blocking mode")
            return process

    def _run_blocking(
        self,
        process: subprocess.Popen,
    ) -> tuple[int, str | None]:
        stderr_logs = []

        # Stream logs
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()
            if stderr_line:
                stderr_logs.append(stderr_line)
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
            stderr_logs.append(line)
            for handler in self.handlers:
                handler.on_job_progress("", line)

        return_code = process.returncode
        logger.debug(f"Return code: {return_code}")
        error_message = None
        if stderr_logs:
            logger.debug(f"Stderr logs: {stderr_logs}")
            error_message = "\n".join(stderr_logs)

            # TODO: remove this once we have a better way to handle errors
            if return_code == 0 and error_message and "| ERROR" in error_message:
                logger.debug("Error detected in logs, even with return code 0.")
                return_code = 1

        # Handle job completion results
        for handler in self.handlers:
            handler.on_job_completion(process.returncode)

        return return_code, error_message


class PythonRunner(SyftRuntime):
    """Runs a Python job in a local subprocess."""

    def run(
        self,
        job_config: JobConfig,
    ) -> tuple[int, str | None] | subprocess.Popen:
        """Run a job"""
        self._validate_paths(job_config)
        self._prepare_job_folders(job_config)

        cmd = self._prepare_run_command(job_config)

        env = os.environ.copy()
        env.update(job_config.get_env())
        env.update(job_config.extra_env)

        return self._run_subprocess(
            cmd, job_config, env=env, blocking=job_config.blocking
        )

    def _prepare_run_command(self, job_config: JobConfig) -> list[str]:
        return [
            *job_config.runtime.cmd,
            str(Path(job_config.function_folder) / job_config.args[0]),
            *job_config.args[1:],
        ]


class DockerRunner(SyftRuntime):
    """Runs a job in a Docker container."""

    def run(
        self,
        job_config: JobConfig,
    ) -> tuple[int, str | None] | subprocess.Popen:
        """Run a job in a Docker container"""
        logger.debug(
            f"Running code in '{job_config.function_folder}' on dataset '{job_config.data_path}' with runtime '{job_config.runtime.kind.value}'"
        )

        self._validate_paths(job_config)
        self._prepare_job_folders(job_config)

        self._check_docker_daemon()
        self._check_or_build_image(job_config)

        cmd = self._prepare_run_command(job_config)

        return self._run_subprocess(cmd, job_config, blocking=job_config.blocking)

    def _check_docker_daemon(self) -> None:
        """Check if the Docker daemon is running."""
        try:
            process = subprocess.run(
                ["docker", "info"],
                check=True,
                capture_output=True,
            )
            logger.info(
                f"Docker daemon is running with return code {process.returncode}"
            )
        except Exception as e:
            if self.update_job_status_callback:
                self.update_job_status_callback(
                    JobStatusUpdate(
                        status=JobStatus.job_run_failed,
                        error=JobErrorKind.execution_failed,
                        error_message="Docker daemon is not running with error: "
                        + str(e),
                    )
                )
            raise RuntimeError("Docker daemon is not running with error: " + str(e))

    def _get_image_name(self, job_config: JobConfig) -> str:
        """Get the Docker image name from the config or use the default."""
        runtime_config = job_config.runtime.config
        if not runtime_config.image_name:
            return job_config.runtime.name
        return runtime_config.image_name

    def _check_or_build_image(self, job_config: JobConfig) -> None:
        """Check if the Docker image exists, otherwise build it."""
        image_name = self._get_image_name(job_config)
        result = subprocess.run(
            ["docker", "image", "inspect", image_name],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode == 0:
            logger.info(f"Docker image '{image_name}' already exists.")
            return

        logger.info(f"Docker image '{image_name}' not found. Building it now...")
        self._build_docker_image(job_config)

    def _build_docker_image(self, job_config: JobConfig) -> None:
        """Build the Docker image."""
        image_name = self._get_image_name(job_config)
        dockerfile_content: str = job_config.runtime.config.dockerfile_content
        error_for_job: str | None = None
        build_context = "."
        try:
            build_cmd = [
                "docker",
                "build",
                "-t",
                image_name,
                "-f",
                "-",  # Use stdin for Dockerfile content
                str(build_context),
            ]
            logger.debug(
                f"Running docker build command: {' '.join(build_cmd)}\nDockerfile content:\n{dockerfile_content}"
            )
            process = subprocess.run(
                build_cmd,
                input=dockerfile_content,
                capture_output=True,
                check=True,
                text=True,
            )

            logger.debug(process.stdout)
            logger.info(f"Successfully built Docker image '{image_name}'.")
        except FileNotFoundError:
            raise RuntimeError("Docker not installed or not in PATH.")
        except subprocess.CalledProcessError as e:
            error_message = f"Failed to build Docker image '{image_name}'."
            logger.error(f"{error_message} stderr: {e.stderr}")
            error_for_job = f"{error_message}\n{e.stderr}"
            raise RuntimeError(f"Failed to build Docker image '{image_name}'.")
        except Exception as e:
            raise RuntimeError(f"An error occurred during Docker image build: {e}")
        finally:
            if error_for_job:
                if self.update_job_status_callback:
                    self.update_job_status_callback(
                        JobStatusUpdate(
                            status=JobStatus.job_run_failed,
                            error=JobErrorKind.execution_failed,
                            error_message=error_for_job,
                        )
                    )

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


# ------- new runtime classes -------
class FolderBasedRuntime:
    """Base class for folder-based syft runtimes that has the following structure:
    └── SyftBox
        ├── datasites/
        │   ├── do1@openmined.org
        │   ├── do2@openmined.org
        │   └── ds@openmined.org
        └── private/
            └── do1@openmined.org/
                ├── syft_datasets/
                │   ├── dataset_01
                │   └── dataset_02
                └── syft_runtimes/
                    ├── runtime_name/
                    │   ├── jobs/
                    │   ├── running/
                    │   ├── done/
                    │   └── config.yaml
    """

    def __init__(self, syftbox_client: SyftBoxClient, runtime_name: str):
        self.syftbox_client = syftbox_client
        self.syft_runtimes_dir = (
            self.syftbox_client.workspace.data_dir
            / "private"
            / self.syftbox_client.email
            / "syft_runtimes"
        )
        self.runtime_dir = self.syft_runtimes_dir / runtime_name
        self.config: BaseRuntimeConfig = BaseRuntimeConfig(
            config_path=self.runtime_dir / "config.yaml"
        )

    @property
    def config_path(self) -> Path:
        """Get the path to the config YAML file."""
        return self.config.config_path

    def init_runtime_dir(self) -> Path:
        """Initialize the runtime directory."""
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir = self.runtime_dir / "jobs"
        self.running_dir = self.runtime_dir / "running"
        self.done_dir = self.runtime_dir / "done"

        self.jobs_dir.mkdir(exist_ok=True)
        self.running_dir.mkdir(exist_ok=True)
        self.done_dir.mkdir(exist_ok=True)

        self.config.save_to_yaml()

        logger.debug(f"Initialized runtime directory: {self.runtime_dir}")

        return self.runtime_dir

    def load_config(self) -> BaseRuntimeConfig:
        """Load the config from the YAML file."""
        return BaseRuntimeConfig.from_yaml(self.config_path)

    def update_config(self, **kwargs) -> None:
        """Update config with new values and save to file.

        Args:
            **kwargs: Config fields to update
        """
        self.config.update(**kwargs)

    def submit_job(self, job_config: JobConfig) -> str:
        """Submit a job to the execution queue"""
        job_id = str(uuid4())
        job_file = self.jobs_dir / f"{job_id}.json"

        # Serialize job config to JSON
        job_data = {
            "job_id": job_id,
            "config": job_config.model_dump_json(),
            "status": JobStatus.pending_code_review.value,
            "created_at": time.time(),
        }

        job_file.write_text(json.dumps(job_data, indent=2))
        logger.info(f"Job {job_id} submitted to {job_file}")
        return job_id

    def get_job_status(self, job_id: str) -> JobStatus:
        """Get the current status of a job"""
        # Check in execution queue
        job_file = self.jobs_dir / f"{job_id}.json"
        if job_file.exists():
            job_data = json.loads(job_file.read_text())
            return JobStatus(job_data["status"])

        # Check in done folder
        done_file = self.done_dir / f"{job_id}.json"
        if done_file.exists():
            job_data = json.loads(done_file.read_text())
            return JobStatus(job_data["status"])

        # Check status updates
        status_file = self.running_dir / f"{job_id}_status.json"
        if status_file.exists():
            status_data = json.loads(status_file.read_text())
            return JobStatus(status_data["status"])

        raise ValueError(f"Job {job_id} not found")

    def get_job_results(self, job_id: str) -> JobResults:
        """Get the results of a completed job"""
        done_file = self.done_dir / f"{job_id}.json"
        if not done_file.exists():
            raise ValueError(f"Job {job_id} is not completed or not found")

        job_data = json.loads(done_file.read_text())
        if job_data["status"] != JobStatus.job_run_finished.value:
            raise ValueError(f"Job {job_id} has not finished successfully")

        results_dir = self.done_dir / f"{job_id}_results"
        if not results_dir.exists():
            raise ValueError(f"Results directory for job {job_id} not found")

        return JobResults(results_dir=results_dir)

    def watch_folders(self) -> None:
        """Start watching the folders for new jobs (non-blocking)"""
        if self._running:
            logger.warning("Folder watching is already running")
            return

        self._running = True
        self._watch_thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._watch_thread.start()
        logger.info("Started folder watching thread")

    def stop_watching(self) -> None:
        """Stop watching folders"""
        self._running = False
        if self._watch_thread:
            self._watch_thread.join(timeout=5)
        logger.info("Stopped folder watching")

    def _watch_loop(self) -> None:
        """Main loop for watching and processing jobs"""
        while self._running:
            try:
                self.process_job_queue()
                time.sleep(1)  # Check every second
            except Exception as e:
                logger.error(f"Error in watch loop: {e}")
                time.sleep(5)  # Wait longer on error

    def process_job_queue(self) -> None:
        """Process all pending jobs in the queue"""
        job_files = list(self.jobs_dir.glob("*.json"))

        for job_file in job_files:
            try:
                job_data = json.loads(job_file.read_text())
                job_id = job_data["job_id"]

                # Skip if not pending
                if job_data["status"] != JobStatus.pending_code_review.value:
                    continue

                logger.info(f"Processing job {job_id}")

                # Load job config
                job_config = JobConfig.model_validate_json(job_data["config"])

                # Execute the job
                self._execute_job(job_id, job_config, job_data)

            except Exception as e:
                logger.error(f"Error processing job file {job_file}: {e}")

    def _execute_job(self, job_id: str, job_config: JobConfig, job_data: dict) -> None:
        """Execute a single job"""
        try:
            # Update status to in progress
            job_data["status"] = JobStatus.job_in_progress.value
            job_file = self.jobs_dir / f"{job_id}.json"
            job_file.write_text(json.dumps(job_data, indent=2))

            # Run the job
            result = self.run(job_config)

            if isinstance(result, tuple):
                return_code, error_message = result
            else:
                # Handle non-blocking case - for folder-based runner, we'll make it blocking
                return_code = result.wait()
                error_message = None
                if return_code != 0:
                    stderr_output = result.stderr.read() if result.stderr else ""
                    error_message = stderr_output

            # Update job status based on result
            if return_code == 0:
                job_data["status"] = JobStatus.job_run_finished.value
                job_data["error"] = JobErrorKind.no_error.value
                job_data["error_message"] = None
            else:
                job_data["status"] = JobStatus.job_run_failed.value
                job_data["error"] = JobErrorKind.execution_failed.value
                job_data["error_message"] = error_message

            job_data["completed_at"] = time.time()

            # Move to done folder
            self.move_to_done(job_id, job_data, job_config)

        except Exception as e:
            logger.error(f"Error executing job {job_id}: {e}")
            job_data["status"] = JobStatus.job_run_failed.value
            job_data["error"] = JobErrorKind.execution_failed.value
            job_data["error_message"] = str(e)
            job_data["completed_at"] = time.time()
            self.move_to_done(job_id, job_data, job_config)

    def move_to_done(self, job_id: str, job_data: dict, job_config: JobConfig) -> None:
        """Move a completed job to the done folder"""
        # Save job metadata to done folder
        done_file = self.done_dir
        done_file.write_text(json.dumps(job_data, indent=2))

        # Copy job results (output and logs) to done folder if they exist
        if job_config.output_dir.exists():
            results_dir = self.done_jobs_dir / f"{job_id}_results"
            if results_dir.exists():
                shutil.rmtree(results_dir)
            shutil.copytree(job_config.job_path, results_dir)

        # Remove from execution queue
        job_file = self.jobs_to_execute_dir / f"{job_id}.json"
        if job_file.exists():
            job_file.unlink()

        logger.info(
            f"Job {job_id} moved to done folder with status {job_data['status']}"
        )

    @abstractmethod
    def _prepare_job_folders(self, job_config: JobConfig) -> None:
        """Prepare job-specific folders - to be implemented by subclasses"""
        pass

    @abstractmethod
    def _validate_paths(self, job_config: JobConfig) -> None:
        """Validate job paths - to be implemented by subclasses"""
        pass

    @abstractmethod
    def _run_subprocess(
        self,
        cmd: list[str],
        job_config: JobConfig,
        env: dict | None = None,
        blocking: bool = True,
    ) -> tuple[int, str | None] | subprocess.Popen:
        """Run subprocess - to be implemented by subclasses"""
        pass


class HighLowRuntime:
    def __init__(
        self,
        highside_client: SyftBoxClient,
        highside_identifier: str,
        lowside_syftbox_client: SyftBoxClient,
    ) -> None:
        self.highside_runtime = FolderBasedRuntime(highside_client, highside_identifier)
        self.lowside_runtime = FolderBasedRuntime(
            lowside_syftbox_client, highside_identifier
        )

    def init_runtime_dir(self) -> None:
        self.highside_runtime.init_runtime_dir()
        self.lowside_runtime.init_runtime_dir()


def get_runner_cls(job_config: JobConfig) -> Type[SyftRuntime]:
    """Factory to get the appropriate runner class for a job config."""
    runtime_kind = job_config.runtime.kind
    if runtime_kind == RuntimeKind.PYTHON:
        return PythonRunner
    elif runtime_kind == RuntimeKind.DOCKER:
        return DockerRunner
    else:
        raise NotImplementedError(f"Unsupported runtime kind: {runtime_kind}")
