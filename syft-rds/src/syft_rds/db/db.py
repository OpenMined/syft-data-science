import json
from pathlib import Path
from uuid import UUID
from pydantic import BaseModel, Field
import sqlite3

from syft_rds.models.dataset import Dataset
from syft_rds.models.base_model import Item
from syft_rds.service.context import BaseRPCContext

    
def get_item_paths(context: BaseRPCContext):
    # TODO: automatically register items by default on definition
    STORABLE_ITEMS = [Dataset]
    return {item.cls_name: context.client.my_datasite / "apps" / context.box.app_name / "items" / item.cls_name for item in STORABLE_ITEMS}

    
def get_connection(context: BaseRPCContext) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for item_name in get_item_paths(context).keys():
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {item_name} (
                id TEXT PRIMARY KEY,
                data JSON
            )
        """)
    conn.commit()
    return conn


class BaseDatabase(BaseModel):
    # this is to allow the connection to be excluded from the model
    class Config:
        arbitrary_types_allowed = True
        
    @classmethod
    def from_context(cls, context: BaseRPCContext, **data):
        data["item_paths"] = get_item_paths(context)
        data["connection"] = get_connection(context)
        return cls(**data)
    
    connection: sqlite3.Connection 
    item_paths: dict[str, Path]
    
    def __post_init__(self):
        for path in self.item_paths.values():
            path.mkdir(parents=True, exist_ok=True)
    
    def create_item(self, item: Item) -> UUID:
        itemtype_path = self.item_paths[item.cls_name]
        item_path = itemtype_path / f"{item.uid}.json"
        item_path.parent.mkdir(parents=True, exist_ok=True)
        with open(item_path, "w") as f:
            f.write(item.model_dump_json(indent=2))
        
        self.connection.execute(f"""
            INSERT INTO {item.cls_name} (id, data) VALUES (?, ?)
        """, (str(item.uid), item.model_dump_json(indent=2)))
        self.connection.commit()    
        return item
    
    def get_item(self, item_type: type[Item], uid: UUID) -> Item | None:
        result = self.connection.execute(f"""
            SELECT data FROM {item_type.cls_name} WHERE id = ?
        """, (str(uid),)).fetchone()
        
        if result is None:
            return None
        # TODO: what do we do here?    
        data = json.loads(result[0])
        
        return item_type.model_validate_json(result[0])

    def list_items(self, item_type: type[Item]) -> list[Item]:
        results = self.connection.execute(f"""
            SELECT data FROM {item_type.cls_name}
        """).fetchall()
        
        items = []
        for result in results:
            items.append(item_type.model_validate_json(result[0]))
        return items

    def update_item(self, item: Item) -> None:
        item_path = self.item_paths[item.cls_name]
        with open(item_path / f"{item.uid}.json", "w") as f:
            f.write(item.model_dump_json(indent=2))
            
        self.connection.execute(f"""
            UPDATE {item.cls_name} 
            SET data = ?
            WHERE id = ?
        """, (item.model_dump_json(indent=2), str(item.uid)))
        self.connection.commit()

    def delete_item(self, item_type: type[Item], uid: UUID) -> None:
        item_path = self.item_paths[item_type.cls_name]
        try:
            (item_path / f"{uid}.json").unlink()
        except FileNotFoundError:
            pass
            
        self.connection.execute(f"""
            DELETE FROM {item_type.cls_name}
            WHERE id = ?
        """, (str(uid),))
        self.connection.commit()