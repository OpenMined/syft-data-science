from uuid import UUID

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import Job, JobCreate, JobUpdate


class JobLocalStore(CRUDLocalStore[Job, JobCreate, JobUpdate]):
    ITEM_TYPE = Job

    def delete_by_id(self, uid: UUID) -> bool:
        """Delete a job by its UUID.

        Args:
            uid: UUID of the job to delete

        Returns:
            True if the job was deleted, False if not found
        """
        return self.store.delete(uid)
