from typing import Final, Type

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import UserCode, UserCodeCreate, UserCodeUpdate


class UserCodeLocalStore(CRUDLocalStore[UserCode, UserCodeCreate, UserCodeUpdate]):
    ITEM_TYPE: Final[Type[UserCode]] = UserCode
