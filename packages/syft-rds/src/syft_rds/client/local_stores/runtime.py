from typing import Final, Type

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models import Runtime, RuntimeCreate, RuntimeUpdate


class RuntimeLocalStore(CRUDLocalStore[Runtime, RuntimeCreate, RuntimeUpdate]):
    ITEM_TYPE: Final[Type[Runtime]] = Runtime
