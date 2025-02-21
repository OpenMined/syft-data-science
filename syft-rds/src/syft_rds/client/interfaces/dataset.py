from typing import Union, Optional
from pathlib import Path

from uuid import UUID

from syft_core import Client as SyftBoxClient

from syft_rds.client.interfaces.base import CRUDInterface
from syft_rds.services.dataset.dataset_models import CreateDataset, UpdateDataset


class DataInterface(CRUDInterface):
    """Interface for managing dataset operations through RPC calls.

    This class extends CRUDInterface to provide specific functionalities for dataset
    management operations including creation, retrieval, updating, and deletion.
    Each method, e.g. `create`, can first implement the client-facing logic
    (e.g. prompting, visualization...) and then call the server-facing methods to make RPC
    calls via the private methods provided by the base class.

    Args:
        host (str): The email of the remote datasite.
        client (Optional[Client]): An instance of the SyftBox Client. If None, it will be loaded from the default configuration.
    """

    def __init__(self, host: str, syftbox_client: SyftBoxClient):
        super().__init__(host, syftbox_client, "dataset")

    def create(
        self,
        name: str,
        path: Union[str, Path],
        mock_path: Union[str, Path],
        summary: Optional[str] = None,
        description_path: Optional[str] = None,
    ):
        create_dataset = CreateDataset(
            name=name,
            private_data_path=path,
            mock_data_path=mock_path,
            summary=summary,
            description_path=description_path,
        )
        return super()._create(create_dataset)

    def get(self, dataset_id: Union[str, UUID]):
        """
        Offline first
            - Check if the dataset is in the local SyftBox
            - Fetch schema file dataset.schema.json
            - Parse the schema and generate a pydantic model that can load the dataset
            - dataset.mock returns a mock path
            - dataset.private = dataset.private
        """
        return super()._get(str(dataset_id))

    def delete(self, dataset_id: Union[str, UUID]):
        return super()._delete(str(dataset_id))

    def update(self, update_dataset: UpdateDataset):
        return super()._update(update_dataset)
