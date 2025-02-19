import enum
from uuid import UUID

from syft_rds.models.base_model import ItemBase, ItemBaseCreate, ItemBaseUpdate


class Dataset(ItemBase):
    name: str
    description: str | None = None


class DatasetCreate(ItemBaseCreate[Dataset]):
    name: str
    description: str | None = None


class DatasetUpdate(ItemBaseUpdate[Dataset]):
    name: str | None = None
    description: str | None = None


class UserCode(ItemBase):
    code_str: str


class UserCodeCreate(ItemBaseCreate[UserCode]):
    code_str: str


class RequestStatus(enum.Enum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class Request(ItemBase):
    code_id: UUID
    kwargs: dict[str, UUID]
    status: RequestStatus = RequestStatus.PENDING


class JobStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class Job(ItemBase):
    code_id: UUID
    kwargs: dict[str, UUID]
    status: JobStatus = JobStatus.PENDING
    result_id: UUID


class JobCreate(ItemBaseCreate[Job]):
    code_id: UUID
    kwargs: dict[str, UUID]

    def to_item(self) -> Job:
        result_id = UUID.uuid4()
        return Job(
            code_id=self.code_id,
            kwargs=self.kwargs,
            result_id=result_id,
            status=JobStatus.PENDING,
        )
