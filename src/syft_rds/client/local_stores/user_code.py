from typing import Final, Type
from uuid import UUID

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import UserCode, UserCodeCreate, UserCodeUpdate


class UserCodeLocalStore(CRUDLocalStore[UserCode, UserCodeCreate, UserCodeUpdate]):
    ITEM_TYPE: Final[Type[UserCode]] = UserCode

    def delete_by_id(self, uid: UUID) -> bool:
        """Delete a user code by its UUID.

        Args:
            uid: UUID of the user code to delete

        Returns:
            True if the user code was deleted, False if not found
        """
        return self.store.delete(uid)
