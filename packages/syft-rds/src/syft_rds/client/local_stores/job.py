from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    ITEM_TYPE = Job
