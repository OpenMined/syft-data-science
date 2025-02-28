from typing import Final, Type

from syft_rds.client.local_stores.base import CRUDLocalStore
from syft_rds.models.models import Dataset, DatasetCreate, DatasetUpdate


class DatasetLocalStore(CRUDLocalStore[Dataset, DatasetCreate, DatasetUpdate]):
    SCHEMA: Final[Type[Dataset]] = Dataset
