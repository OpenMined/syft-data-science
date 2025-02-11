import json
from pathlib import Path
from uuid import UUID
from syft_rds.connection.connection import get_connection
from syft_rds.db.db import BaseQueryEngine, get_item_paths
from syft_rds.models.base_model import Item
from syft_rds.models.dataset import Dataset
from syft_rds.service.context import BaseRPCContext


class SingleFileDBEngine(BaseQueryEngine):
    
    item_type: type[Item]
    
    @property
    def data_path(self) -> Path:
        return self.item_paths[self.item_type.cls_name] / f"{self.item_type.cls_name}.json"
    
    def list_items(self) -> list[Item]:
        data_file = self.data_path
        if not data_file.exists():
            return []
        
        with open(data_file, "r") as f:
            full_json = json.loads(f.read())
            return [self.item_type.model_validate(item) for item in full_json]
        
    def write_data(self, data: list[Item]) -> None:
        data_file = self.data_path
        data_file.parent.mkdir(parents=True, exist_ok=True)
        full_json = json.dumps([item.model_dump(mode="json") for item in data], indent=2)
        with open(data_file, "w") as f:
            f.write(full_json)
    
    def create_item(self, item: Item) -> UUID:
        all_items_path = self.item_paths[item.cls_name]
        all_items_path.parent.mkdir(parents=True, exist_ok=True)
        
        items = self.list_items()
        items.append(item)
        self.write_data(items)
        return item
    
    def get_item(self, uid: UUID) -> Dataset | None:
        items = self.list_items()
        for item in items:
            if item.uid == uid:
                return item
        return None

    def update_item(self, item: Item) -> None:
        items = self.list_items()
        for i, d in enumerate(items):
            if d.uid == item.uid:
                items[i] = item
                break
        self.write_data(items)

    def delete_item(self, uid: UUID) -> None:
        items = self.list_items()
        for i, d in enumerate(items):
            if d.uid == uid:
                items.pop(i)
                break
        self.write_data(items)