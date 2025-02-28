from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    SCHEMA = Job

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_store(self.__class__.SCHEMA)
