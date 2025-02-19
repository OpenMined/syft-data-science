import json
from pathlib import Path
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import ValidationError

from syft_rds.models.base import ItemBase


class StoreError(Exception):
    pass


class ItemNotFoundError(StoreError):
    pass


class ItemExistsError(StoreError):
    pass


class ValidationFailedError(StoreError):
    pass


T = TypeVar("T", bound=ItemBase)


class MultiFileStore(Generic[T]):
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.model_path = self.base_path / f"{self.get_model_name()}"

    def _setup(self):
        self.model_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_model_type(cls) -> type[T]:
        return cls.__orig_bases__[0].__args__[0]

    @classmethod
    def get_model_name(cls) -> str:
        return cls.get_model_type().get_model_name()

    def _path_for_uid(self, uid: UUID) -> Path:
        return self.model_path / f"{uid}.json"

    def create(self, item: T) -> T:
        try:
            path = self._path_for_uid(item.uid)
            if path.exists():
                raise ItemExistsError(f"Item with uid {item.uid} already exists")

            with open(path, "w") as f:
                f.write(item.model_dump_json(indent=2))
            return item
        except (OSError, json.JSONDecodeError) as e:
            raise StoreError(f"Failed to create item: {str(e)}")

    def get(self, uid: UUID) -> T:  # No longer returns None
        try:
            path = self._path_for_uid(uid)
            with open(path) as f:
                data = json.load(f)
            return self.get_model_type().model_validate(data)
        except FileNotFoundError:
            raise ItemNotFoundError(f"Item with uid {uid} not found")
        except json.JSONDecodeError:
            raise StoreError(f"Invalid JSON in file for uid {uid}")
        except ValidationError as e:
            raise ValidationFailedError(f"Invalid data format: {str(e)}")

    def list(self) -> list[T]:
        items = []
        for path in self.model_path.glob("*.json"):
            try:
                with open(path) as f:
                    data = json.load(f)
                items.append(self.get_model_type().model_validate(data))
            except (json.JSONDecodeError, ValidationError, OSError) as e:
                # Log error but continue with other items
                continue
        return items

    def update(self, item: T) -> T:
        try:
            path = self._path_for_uid(item.uid)
            if not path.exists():
                raise ItemNotFoundError(f"Item with uid {item.uid} not found")

            with open(path, "w") as f:
                f.write(item.model_dump_json(indent=2))
            return item
        except (OSError, json.JSONDecodeError) as e:
            raise StoreError(f"Failed to update item: {str(e)}")

    def delete(self, uid: UUID) -> None:
        try:
            path = self._path_for_uid(uid)
            path.unlink()
        except FileNotFoundError:
            raise ItemNotFoundError(f"Item with uid {uid} not found")
        except OSError as e:
            raise StoreError(f"Failed to delete item: {str(e)}")
