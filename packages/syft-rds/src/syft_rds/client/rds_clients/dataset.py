from typing import TYPE_CHECKING, Any, Literal, Optional
from uuid import UUID

from syft_core.types import PathLike
from syft_datasets import Dataset, SyftDatasetManager

from syft_rds.client.local_store import LocalStore
from syft_rds.client.rds_clients.base import RDSClientConfig, RDSClientModule
from syft_rds.client.rds_clients.utils import ensure_is_admin
from syft_rds.client.rpc import RPCClient

if TYPE_CHECKING:
    from syft_rds.client.rds_client import RDSClient


class DatasetRDSClient(RDSClientModule[Dataset]):
    ITEM_TYPE = Dataset

    def __init__(
        self,
        config: RDSClientConfig,
        rpc_client: RPCClient,
        local_store: LocalStore,
        parent: "Optional[RDSClient]" = None,
    ) -> None:
        super().__init__(config, rpc_client, local_store, parent)
        self._dataset_manager = SyftDatasetManager(self._syftbox_client)

    def get_all(
        self,
        order_by: str = "created_at",
        sort_order: str = "desc",
        limit: Optional[int] = None,
        offset: int = 0,
        mode: Literal["local", "rpc"] = "local",
        **filters: Any,
    ) -> list[Dataset]:
        if mode == "rpc":
            raise ValueError("Can only get all datasets in local mode")

        if len(filters) > 0:
            raise ValueError("Filters are not supported")

        return self._dataset_manager.get_all(
            datasite=self.host,
            order_by=order_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
        )

    def get(
        self,
        uid: Optional[UUID] = None,
        mode: Literal["local", "rpc"] = "local",
        name: Optional[str] = None,
        **filters: Any,
    ) -> Dataset:
        if mode == "rpc":
            raise ValueError("Can only get datasets in local mode")

        if name is None:
            raise ValueError("Name must be provided to get a dataset")

        if uid is not None:
            raise ValueError("Cannot get dataset by uid, use name instead")

        return self._dataset_manager.get(
            datasite=self.host,
            name=name,
        )

    @ensure_is_admin
    def create(
        self,
        name: str,
        private_path: PathLike,
        mock_path: PathLike,
        summary: Optional[str] = None,
        readme_path: PathLike | None = None,
        tags: list[str] | None = None,
    ) -> Dataset:
        self._dataset_manager.create(
            name=name,
            mock_path=mock_path,
            private_path=private_path,
            summary=summary,
            readme_path=readme_path,
            tags=tags,
        )

    @ensure_is_admin
    def delete(
        self,
        name: str,
        require_confirmation: bool = True,
    ) -> None:
        """
        Delete a dataset by name.
        If `require_confirmation` is True, it will prompt for confirmation before deletion.
        If `require_confirmation` is False, it will delete without confirmation.
        """
        return self._dataset_manager.delete(
            name=name, datasite=self.host, require_confirmation=require_confirmation
        )
