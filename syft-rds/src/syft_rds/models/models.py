from collections.abc import Iterable
import enum
from pathlib import Path
from uuid import UUID
import uuid

from pydantic import BaseModel, Field, model_validator

from syft_rds.models.base import BaseSchema, BaseSchemaCreate, BaseSchemaUpdate
from syft_rds.utils.name_generator import generate_name


class UserCode(BaseSchema):
    __schema_name__ = "usercode"

    name: str
    path: Path


class UserCodeCreate(BaseSchemaCreate[UserCode]):
    name: str = "My UserCode"
    path: Path


class UserCodeUpdate(BaseSchemaUpdate[UserCode]):
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


class Job(BaseSchema):
    __schema_name__ = "job"

    name: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )  # use a docker like name in the future
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)
    user_metadata: dict = {}
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


class JobCreate(BaseSchemaCreate[Job]):
    name: str = Field(default_factory=generate_name)
    description: str | None = None
    runtime: str
    user_code_id: UUID
    tags: list[str] = Field(default_factory=list)


class JobUpdate(BaseSchemaUpdate[Job]):
    pass


class Runtime(BaseSchema):
    __schema_name__ = "runtime"

    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeCreate(BaseSchemaCreate[Runtime]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class RuntimeUpdate(BaseSchemaUpdate[Runtime]):
    pass


class Dataset(BaseSchema):
    __schema_name__ = "dataset"

    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class DatasetCreate(BaseSchemaCreate[Dataset]):
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)


class DatasetUpdate(BaseSchemaUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None


class GetAllRequest(BaseModel):
    pass
