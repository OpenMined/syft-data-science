from collections.abc import Iterable
import enum
from pathlib import Path
from uuid import UUID
import uuid

from pydantic import BaseModel, Field, Json, model_validator

from syft_rds.models.base import ItemBase, ItemBaseCreate, ItemBaseUpdate
from syft_rds.utils.name_generator import generate_name


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
    name: str = Field(default_factory=generate_name)
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
    private_path: str | Path = Field(description="Private path of the dataset.")
    mock_path: str | Path = Field(description="Mock path of the dataset.")
    file_type: str = Field(description="Type of files in the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | Path | None = Field(
        description="REAMD.md path of the dataset."
    )
    tags: list[str] = Field(description="Tags for the dataset.")

    def get_mock_path(self) -> Path:
        if not Path(self.mock_path).exists():
            raise FileNotFoundError(f"Mock file not found at {self.mock_path}")
        return Path(self.mock_path)

    def get_private_path(self) -> Path:
        """
        Will always raise FileNotFoundError for non-admin since the
        private path will never by synced
        """
        if not Path(self.private_path).exists():
            raise FileNotFoundError(f"Private data not found at {self.private_path}")
        return Path(self.private_path)

    def readme(self) -> str:
        # read the description .md file
        with open(self.description_path) as f:
            return f.read()

    def describe(self):
        # prefix components:
        space = "    "
        branch = "│   "
        # pointers:
        tee = "├── "
        last = "└── "

        def tree(dir_path: Path, prefix: str = ""):
            """A recursive generator, given a directory Path object
            will yield a visual tree structure line by line
            with each line prefixed by the same characters

            Ref: https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
            """
            contents = list(dir_path.iterdir())
            # contents each get pointers that are ├── with a final └── :
            pointers = [tee] * (len(contents) - 1) + [last]
            for pointer, path in zip(pointers, contents):
                yield prefix + pointer + path.name
                if path.is_dir():  # extend the prefix and recurse:
                    extension = branch if pointer == tee else space
                    # i.e. space because last, └── , above so no more |
                    yield from tree(path, prefix=prefix + extension)

        for line in tree(self.get_mock_path().parent):
            print(line)


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str = Field(description="Name of the dataset.")
    path: str = Field(description="Private path of the dataset.")
    mock_path: str = Field(description="Mock path of the dataset.")
    file_type: str = Field(description="Types of files in the dataset.")
    summary: str | None = Field(description="Summary string of the dataset.")
    description_path: str | None = Field(
        description="Path to the detailed REAMD.md of the dataset."
    )
    tags: list[str] | None = Field(description="Tags for the dataset.")


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    pass


class GetOneRequest(BaseModel):
    uid: UUID | None = None


class GetAllRequest(BaseModel):
    pass
