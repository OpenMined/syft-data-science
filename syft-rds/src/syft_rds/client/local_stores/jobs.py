from typing import Final, Type
from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    SCHEMA: Final[Type[Job]] = Job
