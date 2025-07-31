from enum import Enum
from pathlib import Path
import json
import hashlib
from typing import Any, TypeAlias, Union, Literal, Optional
import os
from IPython.display import HTML, display
from datetime import datetime

from loguru import logger
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
)

from syft_notebook_ui.pydantic_html_repr import create_html_repr
import yaml

PathLike: TypeAlias = Union[str, os.PathLike, Path]


class RuntimeKind(str, Enum):
    PYTHON = "python"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"


class BaseRuntimeConfig(BaseModel):
    """Base configuration for runtime environments."""

    config_path: PathLike | None = None
    datasets: list[str] | None = Field(default_factory=list)
    runtime_name: str | None = None
    cmd: list[str] | None = None

    def validate_config(self) -> bool:
        """Override in subclasses for custom validation."""
        return True

    def save_to_yaml(self) -> None:
        """Save the config to a YAML file."""
        if self.config_path is None:
            raise ValueError("config_path is not set")
        yaml_dump = yaml.safe_dump(
            self.model_dump(mode="json", exclude_none=True, exclude={"config_path"}),
            indent=2,
            sort_keys=False,
        )
        self.config_path.write_text(yaml_dump)

    @classmethod
    def from_yaml(cls, config_path: PathLike) -> "BaseRuntimeConfig":
        """Create instance from YAML file."""
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
        config_dict["config_path"] = config_path
        return cls(**config_dict)

    def update(self, **kwargs) -> None:
        """Update config with new values and save to file."""
        config_dict = self.model_dump(mode="json", exclude_none=True)
        config_dict.update(kwargs)
        self.model_validate(config_dict)
        self.save_to_yaml()


class PythonRuntimeConfig(BaseRuntimeConfig):
    version: str | None = None
    requirements_file: PathLike | None = None
    cmd: list[str] = Field(default_factory=lambda: ["python"])

    @field_validator("requirements_file")
    def validate_requirements_file_exist(cls, value: PathLike) -> PathLike:
        if value is not None:
            requirements_file_path = Path(value).expanduser().resolve()
            if not requirements_file_path.exists():
                raise FileNotFoundError(f"Requirements file '{value}' does not exist")
        return value


class DockerMount(BaseModel):
    source: PathLike
    target: PathLike
    mode: Literal["ro", "rw"] = "ro"

    @field_validator("source")
    def validate_source_path_exists(cls, value: PathLike) -> PathLike:
        source_path = Path(value).expanduser().resolve()
        if not source_path.exists():
            # raise FileNotFoundError(f"Source path '{value}' does not exist")
            logger.warning(f"Source path '{value}' does not exist")
        return value


class DockerRuntimeConfig(BaseRuntimeConfig):
    dockerfile: PathLike | None = Field(default=None, exclude=True)
    entrypoint_script: PathLike | None = Field(default=None, exclude=True)
    dockerfile_content: str
    image_name: str | None = None
    cmd: list[str] = ["python"]
    app_name: str | None = None
    extra_mounts: list[DockerMount] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def load_content_from_path(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        If a 'dockerfile' path is provided, read its content into 'dockerfile_content'.
        This validator runs before any other field validation.
        """
        if "dockerfile" in values and values["dockerfile"] is not None:
            dockerfile_path = Path(values["dockerfile"]).expanduser().resolve()
            if not dockerfile_path.exists():
                raise FileNotFoundError(
                    f"The specified Dockerfile does not exist at: {dockerfile_path}"
                )
            values["dockerfile_content"] = dockerfile_path.read_text()
        elif "dockerfile_content" not in values:
            raise ValueError(
                "You must provide a path to a Dockerfile via the 'dockerfile' argument."
            )
        return values

    @field_validator("dockerfile_content")
    def validate_dockerfile_content(cls, dockerfile_content: str) -> str:
        if not dockerfile_content:
            raise ValueError("Dockerfile cannot be empty")
        return dockerfile_content.strip()

    def __hash__(self) -> int:
        return hash(self.dockerfile_content)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, DockerRuntimeConfig):
            return False
        return self.dockerfile_content == __value.dockerfile_content


class KubernetesRuntimeConfig(BaseRuntimeConfig):
    image: str
    namespace: str = "syft-rds"
    num_workers: int = 1
    cmd: list[str] = Field(default_factory=list)


class HighLowRuntimeConfig(BaseRuntimeConfig):
    """Configuration for high-low runtime with dataset tracking."""

    def add_dataset(self, dataset_name: str) -> bool:
        """Add a dataset to the config if not already present.

        Returns:
            True if dataset was added, False if already present
        """
        if dataset_name not in self.datasets:
            self.datasets.append(dataset_name)
            self.save_to_yaml()
            return True
        return False

    def remove_dataset(self, dataset_name: str) -> bool:
        """Remove a dataset from the config.

        Returns:
            True if dataset was removed, False if not found
        """
        if dataset_name in self.datasets:
            self.datasets.remove(dataset_name)
            self.save_to_yaml()
            return True
        return False


RuntimeConfig = (
    PythonRuntimeConfig
    | DockerRuntimeConfig
    | KubernetesRuntimeConfig
    | HighLowRuntimeConfig
)


class Runtime(BaseModel):
    name: str | None = None
    kind: RuntimeKind
    config: RuntimeConfig = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    description: str | None = Field(default=None)

    @model_validator(mode="after")
    def set_name_with_prefix(self):
        if self.name is not None:
            return self
        # Create a unique name for the runtime based on the config's hash
        # TODO: create hash based on Docker file's content
        config_dict = self.config.model_dump(mode="json")
        config_str = json.dumps(config_dict, sort_keys=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:6]
        self.name = f"{self.kind.value.lower()}_{config_hash}"
        return self

    @property
    def cmd(self) -> list[str]:
        return self.config.cmd


class RuntimeCreate(BaseModel):
    name: str | None = None
    kind: RuntimeKind
    config: RuntimeConfig = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    description: str | None = Field(default=None)

    @model_validator(mode="after")
    def set_name_with_prefix(self):
        if self.name is not None:
            return self
        # Create a unique name for the runtime based on the config's hash
        config_dict = self.config.model_dump(mode="json")
        config_str = json.dumps(config_dict, sort_keys=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:6]
        self.name = f"{self.kind.value.lower()}_{config_hash}"
        return self


class RuntimeUpdate(BaseModel):
    pass


class JobConfig(BaseModel):
    """Configuration for a job run"""

    function_folder: Path
    args: list[str]
    data_path: Path
    runtime: Runtime
    job_folder: Optional[Path] = Field(
        default_factory=lambda: Path("jobs") / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    timeout: int = 60
    data_mount_dir: str = "/app/data"
    extra_env: dict[str, str] = {}
    blocking: bool = Field(default=True)

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
            "CODE_DIR": str(self.function_folder.absolute()),
            "TIMEOUT": str(self.timeout),
            "INPUT_FILE": str(self.function_folder / self.args[0]),
            "INTERPRETER": interpreter,
        }


class JobResults(BaseModel):
    _MAX_LOADED_FILE_SIZE: int = 10 * 1024 * 1024  # 10 MB

    results_dir: Path

    @property
    def logs_dir(self) -> Path:
        return self.results_dir / "logs"

    @property
    def output_dir(self) -> Path:
        return self.results_dir / "output"

    @property
    def stderr_file(self) -> Path:
        return self.logs_dir / "stderr.log"

    @property
    def stdout_file(self) -> Path:
        return self.logs_dir / "stdout.log"

    @property
    def stderr(self) -> str | None:
        if self.stderr_file.exists():
            return self.stderr_file.read_text()
        return None

    @property
    def stdout(self) -> str | None:
        if self.stdout_file.exists():
            return self.stdout_file.read_text()
        return None

    @property
    def log_files(self) -> list[Path]:
        return list(self.logs_dir.glob("*"))

    @property
    def output_files(self) -> list[Path]:
        return list(self.output_dir.glob("*"))

    @property
    def outputs(self) -> dict[str, Any]:
        outputs = {}
        for file in self.output_dir.glob("*"):
            try:
                contents = _load_output_file(
                    filepath=file, max_size=self._MAX_LOADED_FILE_SIZE
                )
                outputs[file.name] = contents
            except ValueError as e:
                logger.warning(
                    f"Skipping output {file.name}: {e}. Please load this file manually."
                )
                continue
        return outputs

    def describe(self):
        display_paths = ["output_dir"]
        if self.stdout_file.exists():
            display_paths.append("stdout_file")
        if self.stderr_file.exists():
            display_paths.append("stderr_file")

        html_repr = create_html_repr(
            obj=self,
            fields=["output_dir", "logs_dir"],
            display_paths=display_paths,
        )

        display(HTML(html_repr))


class JobStatus(str, Enum):
    pending_code_review = "pending_code_review"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"
    job_in_progress = "job_in_progress"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class JobErrorKind(str, Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    execution_failed = "execution_failed"
    failed_code_review = "failed_code_review"
    failed_output_review = "failed_output_review"


class JobStatusUpdate(BaseModel):
    status: JobStatus
    error: JobErrorKind
    error_message: str | None = None


def _load_output_file(filepath: Path, max_size: int) -> Any:
    if not filepath.exists():
        raise ValueError(f"File {filepath} does not exist.")

    file_size = filepath.stat().st_size
    if file_size > max_size:
        raise ValueError(
            f"File the maximum size of {int(max_size / (1024 * 1024))} MB."
        )

    if filepath.suffix == ".json":
        with open(filepath, "r") as f:
            return json.load(f)

    elif filepath.suffix == ".parquet":
        import pandas as pd

        return pd.read_parquet(filepath)

    elif filepath.suffix == ".csv":
        import pandas as pd

        return pd.read_csv(filepath)

    elif filepath.suffix in {".txt", ".log", ".md", ".html"}:
        with open(filepath, "r") as f:
            return f.read()

    else:
        raise ValueError("Unsupported file type.")
