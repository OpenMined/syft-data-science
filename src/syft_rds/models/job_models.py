import enum
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from IPython.display import HTML, display
from loguru import logger
from pydantic import BaseModel, Field, model_validator
from syft_core import SyftBoxURL

from syft_rds.display_utils.html_format import create_html_repr
from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.utils.name_generator import generate_name

if TYPE_CHECKING:
    from syft_rds.models.user_code_models import UserCode


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class JobErrorKind(str, enum.Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    execution_failed = "execution_failed"
    failed_code_review = "failed_code_review"
    failed_output_review = "failed_output_review"


class Job(ItemBase):
    class Config:
        extra = "forbid"

    __schema_name__ = "job"
    __table_extra_fields__ = [
        "created_by",
        "name",
        "dataset_name",
        "status",
        "error",
        "error_message",
    ]

    name: str = Field(default_factory=generate_name)
    description: str | None = None
    user_code_id: UUID
    custom_function_id: Optional[UUID] = None
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error
    error_message: str | None = None
    output_url: SyftBoxURL | None = None
    dataset_name: str

    @property
    def user_code(self) -> "UserCode":
        client = self._client
        return client.user_code.get(self.user_code_id)

    def describe(self) -> None:
        html_description = create_html_repr(
            obj=self,
            fields=[
                "uid",
                "created_by",
                "created_at",
                "updated_at",
                "name",
                "description",
                "status",
                "error",
                "error_message",
                "output_path",
                "dataset_name",
                "user_code_id",
            ],
            display_paths=["output_path"],
        )
        display(HTML(html_description))

    def show_user_code(self) -> None:
        user_code = self.user_code
        user_code.describe()

    def get_update_for_return_code(self, return_code: int) -> "JobUpdate":
        if return_code == 0:
            self.status = JobStatus.job_run_finished
        else:
            self.status = JobStatus.job_run_failed
            self.error = JobErrorKind.execution_failed
            self.error_message = (
                "Job execution failed. Please check the logs for details."
            )
        return JobUpdate(
            uid=self.uid,
            status=self.status,
            error=self.error,
            error_message=self.error_message,
        )

    @property
    def output_path(self) -> Path:
        return self.get_output_path()

    def get_output_path(self) -> Path:
        if self.output_url is None:
            raise ValueError("output_url is not set")
        client = self._client
        return self.output_url.to_local_path(
            datasites_path=client._syftbox_client.datasites
        )

    @model_validator(mode="after")
    def validate_status(self):
        if (
            self.status == JobStatus.job_run_failed
            and self.error == JobErrorKind.no_error
        ):
            raise ValueError("error must be set if status is failed")
        return self


class JobUpdate(ItemBaseUpdate[Job]):
    status: Optional[JobStatus] = None
    error: Optional[JobErrorKind] = None
    error_message: Optional[str] = None


class JobCreate(ItemBaseCreate[Job]):
    name: str | None = None
    description: str | None = None
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    dataset_name: str
    custom_function_id: Optional[UUID] = None


class JobResults(BaseModel):
    _MAX_LOADED_FILE_SIZE: int = 10 * 1024 * 1024  # 10 MB

    job: Job
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
                contents = load_output_file(
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


def load_output_file(filepath: Path, max_size: int) -> Any:
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
