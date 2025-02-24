from functools import wraps
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

import yaml
from pydantic import BaseModel, Field
from syft_core import Client
from syft_core.url import SyftBoxURL


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
        self.client = client or Client.load()
        self.spec = spec
        self.app_name = app_name

    @property
    def spec_store_path(self) -> Path:
        store_dir = self.client.api_data(self.app_name) / "store"
        if not store_dir.exists():
            store_dir.mkdir(parents=True, exist_ok=True)
            # TODO create more restrictive permissions for the store directory
            perms_file = store_dir / "syftperm.yaml"
            perms_file.write_text(PERMS)

        return store_dir / self.spec.__spec_name__

    def _get_record_path(self, id: str | UUID) -> Path:
        """Get the full path for a record's YAML file from its ID."""
        return self.spec_store_path / f"{id}.yaml"

    def _save_record(self, record: BaseSpec) -> None:
        """Save a single record to its own YAML file"""
        file_path = self._get_record_path(record.id)
        yaml_dump = yaml.safe_dump(
            record.model_dump(mode="json"), indent=2, sort_keys=False
        )
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
        records = []
        for file_path in self.spec_store_path.glob("*.yaml"):
            _id = file_path.stem
            loaded_record = self._load_record(_id)
            if loaded_record is not None:
                records.append(loaded_record)
        return records

    @ensure_spec_store_exists
    def create(self, item: BaseSpec) -> UUID:
        """
        Create a new record in the store

        Args:
            item: Instance of the model to create

        Returns:
            ID of the created record
        """
        if not isinstance(item, self.spec):
            raise TypeError(f"`record` must be of type {self.spec.__name__}")
        item.id = uuid4()  # Generate a new id during "create"
        self._save_record(item)
        return item.id

    @ensure_spec_store_exists
    def read(self, id: str | UUID) -> BaseSpec:
        """
        Read a record by ID

        Args:
            id: Record ID to fetch

        Returns:
            Record if found, None otherwise
        """
        return self._load_record(id)

    @ensure_spec_store_exists
    def update(self, id: str | UUID, item: BaseSpec) -> BaseSpec | None:
        """
        Update a record by ID

        Args:
            id: Record ID to update
            item: New data to update with

        Returns:
            Updated record if found, None otherwise
        """
        existing_record = self._load_record(id)
        if not existing_record:
            return None

        # Update the record
        updated_record = existing_record.model_copy(
            update=item.model_dump(exclude={"id"})
        )
        self._save_record(updated_record)
        return self._load_record(id)

    @ensure_spec_store_exists
    def delete(self, id: str | UUID) -> bool:
        """
        Delete a record by ID

        Args:
            id: Record ID to delete

        Returns:
            True if record was deleted, False if not found
        """
        file_path = self._get_record_path(id)
        if not file_path.exists():
            return False
        file_path.unlink()
        return True

    @ensure_spec_store_exists
    def query(self, **filters) -> list[BaseSpec]:
        """
        Query records with exact match filters

        Args:
            **filters: Field-value pairs to filter by

        Returns:
            List of matching records
        """
        # TODO optimize later by using database or in-memory indexes, etc?
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

    @ensure_spec_store_exists
    def search(self, query: str, fields: list[str]) -> list[BaseSpec]:
        """
        Search records with case-insensitive partial matching

        Args:
            query: Search string to look for
            fields: List of fields to search in

        Returns:
            List of matching records
        """
        results = []
        query = query.lower()

        for record in self._list_all_records():
            for field in fields:
                val = getattr(record, field, None)
                if val and isinstance(val, str) and query in val.lower():
                    results.append(record)
                    break
        return results
