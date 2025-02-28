from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Runtime, RuntimeCreate, RuntimeUpdate


class RuntimeLocalStore(CRUDLocalStore[Runtime, RuntimeCreate, RuntimeUpdate]):
    SCHEMA = Runtime
