from typing import Final, Type
from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Runtime, RuntimeCreate, RuntimeUpdate


class RuntimeLocalStore(CRUDLocalStore[Runtime, RuntimeCreate, RuntimeUpdate]):
    SCHEMA: Final[Type[Runtime]] = Runtime
