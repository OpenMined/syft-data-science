from pathlib import Path
from typing import Any, Optional, Union
from uuid import UUID

from syft_rds.client.rds_clients.base import RDSClientModule
from syft_rds.client.rds_clients.utils import ensure_is_admin
from syft_rds.models.models import (
    Dataset,
    DatasetCreate,
    DatasetUpdate,
    GetAllRequest,
    GetOneRequest,
)
from syft_runtime.main import CodeRuntime


class DatasetRDSClient(RDSClientModule):
    @ensure_is_admin
    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        summary: Optional[str] = None,
        description_path: Optional[Union[str, Path]] = None,
        tags: list[str] = [],
        runtime: Optional[CodeRuntime] = None,
    ) -> Dataset:
        dataset_create = DatasetCreate(
            name=name,
            path=str(path),
            mock_path=str(mock_path),
            summary=summary,
            description_path=str(description_path) if description_path else None,
            tags=tags,
            runtime=runtime,
        )
        return self.local_store.dataset.create(dataset_create)

    def get(self, uid: Optional[UUID] = None, **filters: Any) -> Dataset:
        return self.local_store.dataset.get(
            GetOneRequest(
                uid=uid,
                filters=filters,
            )
        )

    def get_all(
        self,
        order_by: str = "created_at",
        sort_order: str = "desc",
        limit: Optional[int] = None,
        offset: int = 0,
        **filters: Any,
    ) -> list[Dataset]:
        return self.local_store.dataset.get_all(
            GetAllRequest(
                order_by=order_by,
                sort_order=sort_order,
                limit=limit,
                offset=offset,
                filters=filters,
            )
        )

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
