from syft_rds.models.base import BaseSchema
import yaml
from functools import wraps
from pathlib import Path
from syft_core import Client
from typing import Generic, TypeVar
from typing import Optional
from typing import Type
from uuid import UUID


PERMS = """
- path: '**'
  permissions:
  - admin
  - read
  - write
  user: '*'
"""

T = TypeVar("T", bound=BaseSchema)


def ensure_store_exists(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.store_path.exists():
            self.store_path.mkdir(parents=True, exist_ok=True)
            perms_file = self.store_path.parent / "syftperm.yaml"
            perms_file.write_text(PERMS)  # TODO create more restrictive permissions
        return func(self, *args, **kwargs)

    return wrapper


class YAMLFileSystemDatabase(Generic[T]):
    def __init__(self, schema: Type[T], db_path: str | Path):
        """A lightweight file-based database that stores records as individual YAML files.

        YAMLFileSystemDatabase provides a simple database implementation where each record
        is stored as a separate YAML file in the filesystem. It supports basic CRUD operations,
        querying, and searching capabilities while using Pydantic models for schema validation.

        The database creates a hierarchical directory structure:

        /db_path/
        ├── model1_schema_name/           # Directory for first model type
        │   ├── uuid1.yaml             # Individual record files
        │   └── uuid2.yaml
        ├── model2_schema_name/           # Directory for second model type
        │   ├── uuid3.yaml
        │   └── uuid4.yaml
        └── syftperm.yaml              # Permissions file

        Where:
        - Each record is stored as a separate .yaml file
        - Filenames are UUIDs (e.g., "123e4567-e89b-12d3-a456-426614174000.yaml")
        - All files for a specific model are stored in a dedicated subdirectory named after the model's __schema_name__
        - A syftperm.yaml file is created at the parent level to manage permissions

        Features:
        - CRUD operations (Create, Read, Update, Delete)
        - Query records with exact field matching
        - Case-insensitive search across specified fields
        - Automatic UUID generation for new records
        - Type safety and validation through Pydantic models

        Example:
            ```python
            from pydantic import BaseModel

            class UserSchema(BaseModel):
                __schema_name__ = "users"
                name: str
                email: str

            # Initialize the database
            db = YAMLFileSystemDatabase(UserSchema, "/path/to/db")

            # Create a new user
            user = UserSchema(name="John Doe", email="john@example.com")
            user_id = db.create(user)

            # Query users
            johns = db.query(name="John Doe")
            ```

        Args:
            schema: The Pydantic model class that defines the schema for stored records.
                Must inherit from BaseSchema.
            db_path: Directory path where the database files will be stored.
                    Can be string or Path object.

        Notes:
            - The database automatically creates the necessary directory structure
            - Each model type gets its own subdirectory based on __schema_name__
            - Records must be instances of Pydantic models inheriting from BaseSchema
            - All operations are file-system based for now (no in-memory caching)
            - Suitable for smaller datasets where simple CRUD operations are needed
            - Provides human-readable storage format
        """
        self.schema = schema
        self.db_path = Path(db_path)

    @property
    def store_path(self) -> Path:
        return self.db_path / self.schema.__schema_name__

    def _get_record_path(self, id: str | UUID) -> Path:
        """Get the full path for a record's YAML file from its ID."""
        return self.store_path / f"{id}.yaml"

    def _save_record(self, record: T) -> None:
        """Save a single record to its own YAML file"""
        file_path = self._get_record_path(record.id)
        yaml_dump = yaml.safe_dump(
            record.model_dump(mode="json"),
            indent=2,
            sort_keys=False,
        )
        file_path.write_text(yaml_dump)

    def _load_record(self, id: str | UUID) -> Optional[T]:
        """Load a single record from its own YAML file"""
        file_path = self._get_record_path(id)
        if not file_path.exists():
            return None
        record_dict = yaml.safe_load(file_path.read_text())
        return self.schema.model_validate(record_dict)

    def list_all(self) -> list[T]:
        """List all records in the store"""
        records = []
        for file_path in self.store_path.glob("*.yaml"):
            _id = file_path.stem
            loaded_record = self._load_record(_id)
            if loaded_record is not None:
                records.append(loaded_record)
        return records

    @ensure_store_exists
    def create(self, record: T, overwrite: bool = False) -> T:
        """
        Create a new record in the store

        Args:
            record: Instance of the model to create
            overwrite: If True, overwrite the record if it already exists

        Returns:
            ID of the created record
        """
        if not isinstance(record, self.schema):
            raise TypeError(f"`record` must be of type {self.schema.__name__}")
        file_path = self._get_record_path(record.id)
        if file_path.exists() and not overwrite:
            raise ValueError(f"Record with ID {record.id} already exists")
        self._save_record(record)
        return record

    @ensure_store_exists
    def read(self, id: str | UUID) -> Optional[T]:
        """
        Read a record by ID

        Args:
            id: Record ID to fetch

        Returns:
            Record if found, None otherwise
        """
        return self._load_record(id)

    @ensure_store_exists
    def update(self, id: str | UUID, record: T) -> Optional[T]:
        """
        Update a record by ID

        Args:
            id: Record ID to update
            record: New data to update with

        Returns:
            Updated record if found, None otherwise
        """
        if not isinstance(record, self.schema):
            raise TypeError(f"`record` must be of type {self.schema.__name__}")

        existing_record = self._load_record(id)
        if not existing_record:
            return None

        # Update the record
        updated_record = existing_record.model_copy(
            update=record.model_dump(exclude={"id"})
        )
        self._save_record(updated_record)
        return updated_record

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
    def query(self, **filters) -> list[T]:
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
    def search(self, query: str, fields: list[str]) -> list[T]:
        """
        Search records with case-sensitive partial matching

        Args:
            query: Search string to look for
            fields: List of fields to search in

        Returns:
            List of matching records
        """
        results = []
        query = query

        for record in self.list_all():
            for field in fields:
                val = getattr(record, field, None)
                if val and query in val:
                    results.append(record)
                    break
        return results

    @ensure_store_exists
    def clear(self) -> None:
        """Clear all records in the store"""
        for file_path in self.store_path.glob("*.yaml"):
            file_path.unlink()


class RDSStore(YAMLFileSystemDatabase):
    APP_NAME = "rds"

    def __init__(self, schema: Type[T], client: Client, datasite: Optional[str] = None):
        """A specialized YAML-based database store for RDS (Remote Data Store) that integrates with SyftBox.

        `RDSStore` extends `YAMLFileSystemDatabase` to provide a storage solution specifically designed
        for use with SyftBox's RDS infrastructure. It automatically configures the database path
        using Syft's client API data directory and maintains the same CRUD, query, and search
        capabilities of its parent class.

        Directory structure with the current four schemas (code, dataset, job and runtime):

        <SYFTBOX-WORKSPACE>/datasites/<YOUR-EMAIL>/api_data/
        ├── rds/                     # RDS application root directory
        │   └── store/               # Database root
        │       ├── code/
        │       │   ├── uuid1.yaml
        │       │   └── uuid2.yaml
        │       ├── dataset/
        │       │   ├── uuid3.yaml
        │       │   └── uuid4.yaml
        │       ├── job/
        │       │   ├── uuid5.yaml
        │       │   └── uuid6.yaml
        │       ├── runtime/
        │       │   ├── uuid7.yaml
        │       │   └── uuid8.yaml
        │       └── syftperm.yaml    # Permissions file

        Args:
            schema: The Schema model class for which to initialize the store.
            client: Syft client instance to use.
            datasite: The datasite email to point to. Defaults to the client's email.
        """
        self.schema = schema
        self.client = client
        self.datasite = datasite or self.client.config.email
        self.db_path = (
            self.client.api_data(self.APP_NAME, datasite=self.datasite) / "store"
        )
