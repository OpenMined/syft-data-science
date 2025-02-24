import yaml
from functools import wraps
from pathlib import Path
from pydantic import BaseModel
from pydantic import Field
from syft_core import Client
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar
from uuid import UUID, uuid4


PERMS = """
- path: '**'
  permissions:
  - admin
  - read
  - write
  user: '*'
"""

S = TypeVar("S", bound="BaseSpec")


class BaseSpec(BaseModel):
    """Base specification class that all spec models must inherit from"""

    __spec_name__: str
    id: UUID = Field(default_factory=uuid4)

    class Config:
        arbitrary_types_allowed: bool = True


def ensure_store_exists(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.store_path.exists():
            self.store_path.mkdir(parents=True, exist_ok=True)
            perms_file = self.store_path.parent / "syftperm.yaml"
            perms_file.write_text(PERMS)  # TODO create more restrictive permissions
        return func(self, *args, **kwargs)

    return wrapper


class YAMLFileSystemDatabase(Generic[S]):
    def __init__(self, spec: Type[S], db_path: str | Path):
        """
        Initialize data store for the given spec model at the given root db_path.

        Args:
            spec: The specification model class for which to initialize the store.
            db_path: Directory path to store the database files
        """
        self.spec = spec
        self.db_path = Path(db_path)

    @property
    def store_path(self) -> Path:
        return self.db_path / self.spec.__spec_name__

    def _get_record_path(self, id: str | UUID) -> Path:
        """Get the full path for a record's YAML file from its ID."""
        return self.store_path / f"{id}.yaml"

    def _save_record(self, record: S) -> None:
        """Save a single record to its own YAML file"""
        file_path = self._get_record_path(record.id)
        yaml_dump = yaml.safe_dump(
            record.model_dump(mode="json"),
            indent=2,
            sort_keys=False,
        )
        file_path.write_text(yaml_dump)

    def _load_record(self, id: str | UUID) -> Optional[S]:
        """Load a single record from its own YAML file"""
        file_path = self._get_record_path(id)
        if not file_path.exists():
            return None
        record_dict = yaml.safe_load(file_path.read_text())
        return self.spec.model_validate(record_dict)

    def list_all(self) -> list[S]:
        """List all records in the store"""
        records = []
        for file_path in self.store_path.glob("*.yaml"):
            _id = file_path.stem
            loaded_record = self._load_record(_id)
            if loaded_record is not None:
                records.append(loaded_record)
        return records

    @ensure_store_exists
    def create(self, item: S) -> UUID:
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

    @ensure_store_exists
    def read(self, id: str | UUID) -> Optional[S]:
        """
        Read a record by ID

        Args:
            id: Record ID to fetch

        Returns:
            Record if found, None otherwise
        """
        return self._load_record(id)

    @ensure_store_exists
    def update(self, id: str | UUID, item: S) -> Optional[S]:
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

    @ensure_store_exists
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

    @ensure_store_exists
    def query(self, **filters) -> list[S]:
        """
        Query records with exact match filters

        Args:
            **filters: Field-value pairs to filter by

        Returns:
            List of matching records
        """
        # TODO optimize later by using database or in-memory indexes, etc?
        results = []

        for record in self.list_all():
            matches = True
            for key, value in filters.items():
                if not hasattr(record, key) or getattr(record, key) != value:
                    matches = False
                    break
            if matches:
                results.append(record)
        return results

    @ensure_store_exists
    def search(self, query: str, fields: list[str]) -> list[S]:
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

        for record in self.list_all():
            for field in fields:
                val = getattr(record, field, None)
                if val and isinstance(val, str) and query in val.lower():
                    results.append(record)
                    break
        return results


class RDSStore(YAMLFileSystemDatabase):
    APP_NAME = "rds"

    def __init__(self, spec: Type[S], client: Optional[Client] = None):
        """
        Initialize RDS data store for the given spec model.

        Args:
            spec: The specification model class for which to initialize the store.
            client: Syft client instance to use.
        """
        self.spec = spec
        self.client = client or Client.load()
        self.db_path = self.client.api_data(self.APP_NAME) / "store"
