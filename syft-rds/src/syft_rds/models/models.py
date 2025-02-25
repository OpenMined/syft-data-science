from collections.abc import Iterable
import enum
from pathlib import Path
from uuid import UUID
import uuid

from pydantic import BaseModel, Field, Json, model_validator

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate


class UserCode(ItemBase):
    name: str
    path: Path


class UserCodeCreate(ItemBaseCreate[UserCode]):
    name: str = "My UserCode"
    path: Path


class UserCodeUpdate(ItemBaseUpdate[UserCode]):
    pass


class JobErrorKind(str, enum.Enum):
    no_error = "no_error"
    timeout = "timeout"
    cancelled = "cancelled"
    error = "error"
    code_review_rejected = "code_review_rejected"
    output_review_rejected = "output_review_rejected"
    failed_review = "failed_review"


class JobArtifactKind(str, enum.Enum):
    computation_result = "computation_result"
    error_log = "error_log"
    execution_log = "execution_log"


class JobStatus(str, enum.Enum):
    pending_code_review = "pending_code_review"
    queued = "queued"
    running = "running"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class Job(ItemBase):
    name: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )  # use a docker like name in the future
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    user_metadata: Json = {}
    status: JobStatus = JobStatus.pending_code_review
    error: JobErrorKind = JobErrorKind.no_error

    @property
    def user_code(self) -> UserCode:
        if self.user_code_id in self._client_cache:
            return self._client_cache[self.user_code_id]
        else:
            raise Exception("UserCode not found")

    class Config:
        extra = "forbid"

    def add_to_queue(self):
        if self.status != JobStatus.pending_code_review:
            raise ValueError(
                "job must be pending code review - call submit_for_code_review() first"
            )
        self.status = JobStatus.queued

    def reject(self, reason: str = "unknown reason"):
        # artifacts are not shared on rejection
        self.status = JobStatus.rejected
        self.error = JobErrorKind.failed_review

    def share_artifacts(
        self,
        include: Iterable[JobArtifactKind] = (
            JobArtifactKind.computation_result,
            JobArtifactKind.error_log,
            JobArtifactKind.execution_log,
        ),
    ):
        if self.status not in (
            JobStatus.job_run_finished,
            JobStatus.job_run_failed,
        ):
            raise ValueError(
                f"Job must be executed first. Current status: {self.status.value}"
            )
        self.status = JobStatus.shared

    @model_validator(mode="after")
    def validate_status(self):
        if (
            self.status == JobStatus.job_run_failed
            and self.error == JobErrorKind.no_error
        ):
            raise ValueError("error must be set if status is failed")
        return self


class JobCreate(ItemBaseCreate[Job]):
    name: str
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)


class JobUpdate(ItemBaseUpdate[Job]):
    pass


class Runtime(ItemBase):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeCreate(ItemBaseCreate[Runtime]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeUpdate(ItemBaseUpdate[Runtime]):
    pass


class Dataset(ItemBase):
    name: str = Field(description="Name of the dataset.")
    private: str = Field(description="Private path of the dataset.")
    mock: str = Field(description="Mock path of the dataset.")
    file_type: str = Field(description="Type of files in the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(description="REAMD.md path of the dataset.")

    def describe(self) -> str:
        # read the description .md file
        with open(self.description_path) as f:
            return f.read()


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    file_type: str = Field(description="Type of files in the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(description="REAMD.md path of the dataset.")
    # tags: list[str] = Field(description="Tags for the dataset.")


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None


class GetAllRequest(BaseModel):
    pass
