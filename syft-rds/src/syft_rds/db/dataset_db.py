import json
from pathlib import Path
from uuid import UUID
from syft_rds.db.db import BaseQueryEngine
from syft_rds.models.dataset import Dataset


class DatasetQueryEngine(BaseQueryEngine):
    
    @property
    def datasets_path(self) -> Path:
        return self.item_paths["SyftDataset"] / "datasets.json"
    
    def list_items(self) -> list[Dataset]:
        datasets_file = self.datasets_path
        if not datasets_file.exists():
            return []
        
        with open(datasets_file, "r") as f:
            full_json = json.loads(f.read())
            return [Dataset.model_validate(item) for item in full_json]
        
    def write_datasets(self, datasets: list[Dataset]) -> None:
        datasets_file = self.datasets_path
        datasets_file.parent.mkdir(parents=True, exist_ok=True)
        full_json = json.dumps([dataset.model_dump(mode="json") for dataset in datasets], indent=2)
        print("full_json", full_json)
        with open(datasets_file, "w") as f:
            f.write(full_json)
    
    def create_item(self, item: Dataset) -> UUID:
        dataset_path = self.item_paths[item.cls_name]
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        
        datasets = self.list_items()
        datasets.append(item)
        self.write_datasets(datasets)
        return item
    
    def get_item(self, uid: UUID) -> Dataset | None:
        datasets = self.list_items()
        for dataset in datasets:
            if dataset.uid == uid:
                return dataset
        return None

    def update_item(self, item: Dataset) -> None:
        datasets = self.list_items()
        for i, d in enumerate(datasets):
            if d.uid == item.uid:
                datasets[i] = item
                break
        self.write_datasets(datasets)

    def delete_item(self, uid: UUID) -> None:
        datasets = self.list_items()
        for i, d in enumerate(datasets):
            if d.uid == uid:
                datasets.pop(i)
                break
        self.write_datasets(datasets)