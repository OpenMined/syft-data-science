import json
from pathlib import Path
from typing import ClassVar
from uuid import UUID
from syft_rds.db.db import BaseQueryEngine
from syft_rds.models.base_model import Item


class MultiFileDBEngine(BaseQueryEngine):
    
    item_type: ClassVar[type[Item]]
    
    @property
    def data_dir(self) -> Path:
        return self.item_paths[self.item_type.cls_name]
    
    def list_items(self) -> list[Item]:
        data_file = self.data_dir
        items = []
        for item_file in data_file.glob("*.json"):
            with open(item_file, "r") as f:
                item_data = json.loads(f.read())
                items.append(self.item_type.model_validate(item_data))
        return items
        
    def create_item(self, item: Item) -> UUID:
        item_path = self.data_dir / f"{item.uid}.json"
        item_path.parent.mkdir(parents=True, exist_ok=True)
        with open(item_path, "w") as f:
            f.write(item.model_dump_json(indent=2))
    
    def get_item(self, uid: UUID) -> Item | None:
        item_path = self.data_dir / f"{uid}.json"
        if not item_path.exists():
            return None
        with open(item_path, "r") as f:
            item_data = json.loads(f.read())
            return self.item_type.model_validate(item_data)

    def update_item(self, item: Item) -> None:
        item_path = self.data_dir / f"{item.uid}.json"
        with open(item_path, "w") as f:
            f.write(item.model_dump_json(indent=2))
    
    def delete_item(self, uid: UUID) -> None:
        item_path = self.data_dir / f"{uid}.json"
        item_path.unlink()
