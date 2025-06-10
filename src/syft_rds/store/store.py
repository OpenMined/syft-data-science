from functools import wraps
from pathlib import Path
from typing import Generic, Optional, Type, TypeVar
from uuid import UUID

import yaml
from pydantic import TypeAdapter

from syft_rds.models.base import ItemBase

PERMS = """
rules:
- pattern: '**'
  access:
    read:
    - '*'
"""

T = TypeVar("T", bound=ItemBase)


def ensure_store_exists(func):
    @wraps(func)
    def wrapper(self: "YAMLStore", *args, **kwargs):
        if not self.item_type_dir.exists():
            self.item_type_dir.mkdir(parents=True, exist_ok=True)
            perms_file = self.item_type_dir.parent / "syft.pub.yaml"
            perms_file.write_text(PERMS)  # TODO create more restrictive permissions
        return func(self, *args, **kwargs)

    return wrapper


class YAMLStore(Generic[T]):
    def __init__(self, item_type: Type[T], store_dir: str | Path):
        """A lightweight file-based database that stores records as individual YAML files.

        YAMLStore provides a simple database implementation where each record
        is stored as a separate YAML file in the filesystem. It supports basic CRUD operations,
        querying, and searching capabilities while using Pydantic models for  validation.

        The database creates a hierarchical directory structure:

        /store_dir/
        ├── model1_name/           # Directory for first model type
        │   ├── uuid1.yaml             # Individual record files
        │   └── uuid2.yaml
        ├── model2_name/           # Directory for second model type
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

            class User(BaseModel):
                __schema_name__ = "users"
                name: str
                email: str

            # Initialize the database
            store = YAMLStore(User, "/path/to/store")

            # Create a new user
            user = User(name="John Doe", email="john@example.com")
            user_id = store.create(user)

            # Query users
            johns = store.get_all(filters={"name": "John Doe"})
            ```

        Args:
            schema: The Pydantic model class that defines the schema for stored records.
                Must inherit from ItemBase.
            store_dir: Directory path where the database files will be stored.
                    Can be string or Path object.

        Notes:
            - The database automatically creates the necessary directory structure
            - Each model type gets its own subdirectory based on __schema_name__
            - Records must be instances of Pydantic models inheriting from ItemBase
            - All operations are file-system based for now (no in-memory caching)
            - Suitable for smaller datasets where simple CRUD operations are needed
            - Provides human-readable storage format
        """
        self.item_type = item_type
        self.store_dir = Path(store_dir)
        self._field_validators = self._make_field_validators()

    def _make_field_validators(self) -> dict:
        """
        Create a dictionary of field_name: TypeAdapter for each field in the schema.
        These can be used to validate and convert field values to the correct type, required when querying the store.
        """
        return {
            field_name: TypeAdapter(field_info.annotation)
            for field_name, field_info in self.item_type.model_fields.items()
        }

    def _coerce_field_types(self, filters: dict) -> dict:
        """
        If possible, convert filter values to the correct type for the schema.
        e.g. convert str to UUID, or str to Enum, etc.
        """
        # TODO move filter type coercion to YAMLStore to avoid duplication serverside
        resolved_filters = {}
        for filter_name, filter_value in filters.items():
            validator = self._field_validators.get(filter_name, None)
            if validator is None:
                # Cannot infer type, leave it in the original form
                resolved_filters[filter_name] = filter_value
                continue
            try:
                type_adapter = self._field_validators[filter_name]
                validated_value = type_adapter.validate_python(filter_value)
                resolved_filters[filter_name] = validated_value
            except Exception:
                # Cannot convert to the correct type, leave it in the original form
                # logger.exception(
                #     f"Could not convert filter value {filter_value} to {field_info.annotation} for field {filter_name}"
                # )
                resolved_filters[filter_name] = filter_value
        return resolved_filters

    @property
    def item_type_dir(self) -> Path:
        return self.store_dir / self.item_type.__schema_name__

    def _get_record_path(self, uid: str | UUID) -> Path:
        """Get the full path for a record's YAML file from its UID."""
        return self.item_type_dir / f"{uid}.yaml"

    def _save_record(self, record: T) -> None:
        """Save a single record to its own YAML file"""
        file_path = self._get_record_path(record.uid)
        yaml_dump = yaml.safe_dump(
            record.model_dump(mode="json"),
            indent=2,
            sort_keys=False,
        )
        file_path.write_text(yaml_dump)

    @ensure_store_exists
    def get_by_uid(self, uid: str | UUID) -> Optional[T]:
        """Get a single record by UID"""
        file_path = self._get_record_path(uid)
        if not file_path.exists():
            return None
        record_dict = yaml.safe_load(file_path.read_text())
        return self.item_type.model_validate(record_dict)

    @ensure_store_exists
    def list_all(self) -> list[T]:
        """List all records in the store"""
        records = []
        for file_path in self.item_type_dir.glob("*.yaml"):
            _id = file_path.stem
            loaded_record = self.get_by_uid(_id)
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
            UID of the created record
        """
        if not isinstance(record, self.item_type):
            raise TypeError(f"`record` must be of type {self.item_type.__name__}")
        file_path = self._get_record_path(record.uid)
        if file_path.exists() and not overwrite:
            raise ValueError(f"Record with UID {record.uid} already exists")
        self._save_record(record)
        return record

    @ensure_store_exists
    def update(self, uid: str | UUID, record: T) -> Optional[T]:
        """
        Update a record by UID

        Args:
            uid: Record UID to update
            record: New data to update with

        Returns:
            Updated record if found, None otherwise
        """
        if not isinstance(record, self.item_type):
            raise TypeError(f"`record` must be of type {self.item_type.__name__}")

        existing_record = self.get_by_uid(uid)
        if not existing_record:
            return None

        # Update the record
        updated_record = existing_record.model_copy(
            update=record.model_dump(exclude={"uid"})
        )
        self._save_record(updated_record)
        return updated_record

    @ensure_store_exists
    def delete(self, uid: str | UUID) -> bool:
        """
        Delete a record by UID

        Args:
            uid: Record UID to delete

        Returns:
            True if record was deleted, False if not found
        """
        file_path = self._get_record_path(uid)
        if not file_path.exists():
            return False
        file_path.unlink()
        return True

    @ensure_store_exists
    def get_one(self, **filters) -> Optional[T]:
        """
        Get one record with exact match filters.

        Args:
            **filters: Field-value pairs to filter by

        Returns:
            Matching record or None
        """
        if len(filters.keys()) == 1 and "uid" in filters:
            return self.get_by_uid(filters["uid"])

        else:
            res = self.get_all(filters=filters, limit=1)
            if len(res) == 0:
                return None
            return res[0]

    def _sort_items(self, items: list[T], order_by: str, sort_order: str) -> list[T]:
        return sorted(
            items,
            key=lambda x: getattr(x, order_by, None),
            reverse=sort_order == "desc",
        )

    @ensure_store_exists
    def get_all(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: Optional[str] = None,
        sort_order: str = "asc",
        filters: Optional[dict] = None,
    ) -> list[T]:
        """
        Get all records with optional filtering, sorting, and pagination.
        Filters are case-sensitive and must match exactly.

        Args:
            limit (Optional[int], optional): limit. Defaults to None.
            offset (int, optional): offset. Defaults to 0.
            order_by (Optional[str], optional): field to order by. Defaults to None.
            sort_order (str, optional): 'asc' or 'desc'. Defaults to "asc".
            filters (Optional[dict], optional): dictionary of filters, Pydantic is used for type coercion,
                so comparing strings to UUIDs or dates will work. Defaults to None.

        Returns:
            list[T]: List of matching records
        """
        results = []

        filters = self._coerce_field_types(filters or {})
        for record in self.list_all():
            matches = True
            for key, value in filters.items():
                if not hasattr(record, key) or getattr(record, key) != value:
                    matches = False
                    break
            if matches:
                results.append(record)

        if order_by:
            results = self._sort_items(results, order_by, sort_order)
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]

        return results

    @ensure_store_exists
    def text_search(self, query: str, fields: list[str]) -> list[T]:
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
        for file_path in self.item_type_dir.glob("*.yaml"):
            file_path.unlink()
