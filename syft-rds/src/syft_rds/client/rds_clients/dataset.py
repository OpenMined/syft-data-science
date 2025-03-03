from pathlib import Path
from typing import Optional, Union
from functools import wraps

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.models.models import Dataset, DatasetCreate, DatasetUpdate, GetAllRequest


def ensure_is_admin(func):
    """
    Decorator to ensure the user is an admin before executing a function.
    Admin status is determined by comparing the SyftBox client email with the configured host.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._syftbox_client.email != self.config.host:
            raise PermissionError(
                f"You must be the datasite admin to perform this operation. "
                f"Your SyftBox email: '{self._syftbox_client.email}'. "
                f"Host email: '{self.config.host}'"
            )
        return func(self, *args, **kwargs)

    return wrapper


class DatasetRDSClient(RDSClientModule):
    @ensure_is_admin
    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        file_type: str,
        summary: Optional[str] = None,
        description_path: Optional[Union[str, Path]] = None,
        tags: list[str] = [],
    ) -> Dataset:
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            file_type=file_type,
            summary=summary,
            description_path=str(description_path) if description_path else None,
            tags=tags,
        )
        return self.local_store.dataset.create(dataset_create)

    def get(self, name: str) -> Dataset:
        return self.local_store.dataset.get_by_name(name)

    def get_all(self) -> list[Dataset]:
        return self.local_store.dataset.get_all(GetAllRequest())

    @ensure_is_admin
    def delete(self, name: str) -> bool:
        """Delete a dataset by name.

        Args:
            name: Name of the dataset to delete

        Returns:
            True if deletion was successful, False otherwise

        Raises:
            RuntimeError: If deletion fails due to file system errors
        """
        return self.local_store.dataset.delete_by_name(name)

    @ensure_is_admin
    def update(self, dataset_update: DatasetUpdate) -> Dataset:
        raise NotImplementedError("Dataset update is not supported yet")
