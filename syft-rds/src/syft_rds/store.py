from uuid import uuid4
from uuid import UUID
from pathlib import Path
from typing import Any, Optional
from syft_core import Client
from syft_core.url import SyftBoxURL
from pydantic import BaseModel
from pydantic import Field
from functools import wraps
import yaml


PERMS = """
- path: '**'
  permissions:
  - admin
  - read
  - write
  user: '*'
"""


class BaseSpec(BaseModel):
    __spec_name__: str
    id: UUID = Field(default_factory=uuid4)

    class Config:
        arbitrary_types_allowed: bool = True


class DatasetSpec(BaseSpec):
    __spec_name__ = "dataset"
    
    name: str
    description: str
    data: SyftBoxURL
    mock: SyftBoxURL
    tags: list[str]

class StoreSpecNotFoundError(FileNotFoundError):
    pass


# class CodeSpec(Base):
#     language: str
#     path: SyftBoxURL
#     created_by: EmailStr


# class RuntimeType(StrEnum):
#     docker = "docker"
#     # k8s = "k8s"
#     # local = "local"


# class RuntimeSpec(Base):
#     name: str
#     type: RuntimeType
#     kwargs: dict


# class JobSpec(Base):
#     code: str
#     code_hash: str
#     dataset: str
#     dataset_hash: str
#     runtime: str
#     approval_status: str
#     reason: str
#     created_by: str
#     approved_by: str
#     created_at: str
#     approved_at: str
#     tags: str
#     result_path: str
#     result_hash: str
#     logs: str
#     environment: str


def ensure_spec_store_exists(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.spec_store_path.exists():
            self.spec_store_path.mkdir(parents=True, exist_ok=True)
        return func(self, *args, **kwargs)
    return wrapper


class RDSFileStore:
    def __init__(
        self,
        app_name: str,
        spec: BaseSpec,
        client: Optional[Client] = None,
    ):
        self.app_name = app_name
        if client is None:
            self.client = Client.load()
        self.app_dir = self.client.api_data(self.app_name)
        self.app_store_dir = self.app_dir / "store"
        self.spec = spec

        # Create restrictive Permissions for the store Directory
        perms = self.app_store_dir / "syftperm.yaml"
        perms.write_text(PERMS)


    @property
    def store_dir(self) -> Path:
        return self.app_store_dir

    @property
    def spec_store_path(self) -> Path:
        return self.store_dir / self.spec.__spec_name__

    def _get_record_path(self, id: str | UUID) -> Path:
        """Get the full path for a record's YAML file from it's ID."""
        return self.spec_store_path / f"{id}.yaml"

    def _save_record(self, record: BaseSpec) -> None:
        """Save a single record to its own YAML file"""
        file_path = self._get_record_path(record.id)
        yaml_dump = yaml.safe_dump(record.model_dump(mode="json"), indent=2)
        file_path.write_text(yaml_dump)

    def _load_record(self, id: str | UUID) -> BaseSpec:
        """Load a single record from its own YAML file"""
        file_path = self._get_record_path(id)
        if not file_path.exists():
            return None
        record_dict = yaml.safe_load(file_path.read_text())
        return self.spec.model_validate(**record_dict)

    def _list_all_records(self) -> list[BaseSpec]:
        """List all records in the store"""
        records= []
        for file_path in self.spec_store_path.glob("*.yaml"):
            _id = file_path.stem
            records.append(self._load_record(_id))
        return records

    @ensure_spec_store_exists
    def create(self, record: BaseSpec) -> str:
        if not isinstance(record, self.spec):
            raise TypeError(f"`record` must be of type {self}")
        record.id = uuid4()  # Generate a new id during "create"
        self._save_record(record)
        return record.id

    @ensure_spec_store_exists
    def read(self, id: str | UUID) -> BaseSpec:
        return self._load_record(id)

    @ensure_spec_store_exists
    def update(self, id: str | UUID, item: BaseSpec) -> BaseSpec|None:
        existing_record = self._load_record(id)
        if not existing_record:
            return None

        # Update the record
        record = {
            **existing_record.model_dump_json(),
            **item.model_dump_json(),
            "id": id
        }
        self._save_record(record)
        return self._load_record(id)

    @ensure_spec_store_exists
    def delete(self, id: str | UUID) -> bool:
        file_path = self._get_record_path(id)
        if not file_path.exists():
            return False
        file_path.unlink()
        return True

    @ensure_spec_store_exists
    def query(self, **filters) -> list[BaseSpec]:
        results = []

        for record in self._list_all_records():
            matches = True
            for key, value in filters.items():
                if key not in record or getattr(record, key) != value:
                    matches = False
                    break
            if matches:
                results.append(record)
        return results
