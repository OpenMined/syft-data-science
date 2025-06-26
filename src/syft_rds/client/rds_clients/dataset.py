from pathlib import Path
from typing import Optional, Union
from uuid import UUID

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.rds_clients.utils import ensure_is_admin
from syft_rds.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
)


class DatasetRDSClient(RDSClientModule[Dataset]):
    ITEM_TYPE = Dataset

    @ensure_is_admin
    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        summary: Optional[str] = None,
        description_path: Optional[Union[str, Path]] = None,
        tags: list[str] = [],
        runtime_id: Optional[UUID] = None,
    ) -> Dataset:
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            summary=summary,
            description_path=str(description_path) if description_path else None,
            tags=tags,
            runtime_id=runtime_id,
        )
        return self.local_store.dataset.create(dataset_create)

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
