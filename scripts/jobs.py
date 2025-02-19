import enum
from time import sleep
from typing import Iterable
import uuid
from pydantic import BaseModel, Field, Json, field_validator, model_validator


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
    draft = "draft"  # job is created, but not yet submitted
    pending_code_review = "pending_code_review"
    queued = "queued"
    running = "running"
    job_run_failed = "job_run_failed"
    job_run_finished = "job_run_finished"

    # end states
    rejected = "rejected"  # failed to pass the review
    shared = "shared"  # shared with the user


class Dataset(BaseModel):
    name: str
    description: str = "No description"
    user_metadata: Json = {}


class Job(BaseModel):
    name: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )  # use a docker like name in the future
    description: str = "No description"
    user_metadata: Json = {}
    status: JobStatus = JobStatus.draft
    error: JobErrorKind = JobErrorKind.no_error

    class Config:
        extra = "forbid"

    def submit_for_code_review(self):
        if self.status != JobStatus.draft:
            raise ValueError("job must be draft to be submitted for code review")
        self.status = JobStatus.pending_code_review

    def add_to_queue(self):
        if self.status != JobStatus.pending_code_review:
            raise ValueError("job must be pending code review to be added to queue")
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
                "Job must be completed or failed to be shared. "
                f"Current status: {self.status}"
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


class UserCode(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_ids: list[str]
    dataset_id: str
    user_metadata: Json = {}


jobs = [
    Job(status=JobStatus.pending_code_review),
    Job(status=JobStatus.job_run_finished),
    Job(status=JobStatus.job_run_failed, error=JobErrorKind.failed_review),
]


def get_all(**kwargs):
    for job in jobs:
        if all(getattr(job, k) == v for k, v in kwargs.items()):
            yield job


if __name__ == "__main__":
    from unittest.mock import Mock

    client = Mock()
    client.jobs.get_all = get_all

    for job in client.jobs.get_all(status=JobStatus.pending_code_review):
        print(job)
        # print(user_code) # code review
        job.add_to_queue()

        # job is run by the runner
        # polling the runner for the status
        while job.status not in (
            JobStatus.job_run_finished,
            JobStatus.job_run_failed,
        ):
            # we can stream logs here
            sleep(1)

        job.share_artifacts(  # auto share by default
            include=[  # include all by default
                JobArtifactKind.computation_result,
                JobArtifactKind.error_log,
                JobArtifactKind.execution_log,
            ]
        )
        print(job)

        job.reject("some reason")
