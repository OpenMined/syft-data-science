import json
from pathlib import Path
from typing import ClassVar
from uuid import UUID
from syft_rds.db.db import BaseQueryEngine
from syft_rds.db.single_file_db import SingleFileDBEngine
from syft_rds.models.base_model import Item
from syft_rds.models.dataset import Dataset


class DatasetQueryEngine(SingleFileDBEngine):
    
    item_type: type[Item] = Dataset
