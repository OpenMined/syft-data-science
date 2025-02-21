from typing import Optional, Union

from uuid import UUID

from syft_core import Client

from syft_rds.client.interfaces.base import CRUDInterface
from syft_rds.services.dataset.dataset_models import CreateDataset, UpdateDataset


class DatasetInterface(CRUDInterface):
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

    def __init__(self, host: str, client: Optional[Client] = None):
        super().__init__(host, "dataset", client)

    def create(self, create_dataset: CreateDataset):
        return super()._create(create_dataset)

    def get(self, dataset_id: Union[str, UUID]):
        return super()._get(str(dataset_id))

    def delete(self, dataset_id: Union[str, UUID]):
        return super()._delete(str(dataset_id))

    def update(self, update_dataset: UpdateDataset):
        return super()._update(update_dataset)
