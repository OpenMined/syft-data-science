from pathlib import Path
from uuid import uuid4, UUID
from datetime import datetime

import yaml
from loguru import logger
from syft_event import SyftEvents
from syft_event.types import Request
from syft_rds.store.rds_store import RDSStore

from syft_rds.services.dataset.dataset_model import (
    CreateDataset,
    Dataset,
    DatasetResponse,
)


APP_NAME = "dataset"

box = SyftEvents(APP_NAME)
dataset_store = RDSStore(APP_NAME)


def convert_mock_to_syftbox_path(mock_data_path: str, dataset_id: UUID) -> str:
    store_dir = dataset_store.store_dir
    dataset_dir = store_dir / str(dataset_id)
    dataset_dir.mkdir(exist_ok=True)

    # Copy Mock data to dataset directory
    mock_data_path: Path = Path(mock_data_path)
    final_mock_path = dataset_dir / mock_data_path.name
    final_mock_path.write_bytes(mock_data_path.read_bytes())

    return f"syft://{dataset_store.client.email}/api_data/{APP_NAME}/store/{str(dataset_id)}/{final_mock_path.name}"


def convert_private_to_syftbox_path(private_data_path: str, dataset_id: UUID) -> str:
    private_dir = dataset_store.client.workspace.data_dir / "private"
    private_dir.mkdir(exist_ok=True, parents=True)
    dataset_dir = private_dir / APP_NAME / str(dataset_id)
    dataset_dir.mkdir(exist_ok=True, parents=True)

    # Copy Private data to dataset directory
    private_data_path: Path = Path(private_data_path)
    final_private_path: Path = dataset_dir / private_data_path.name
    final_private_path.write_bytes(private_data_path.read_bytes())

    return f"syft://private/{APP_NAME}/{str(dataset_id)}/{final_private_path.name}"


@box.on_request("/create")
def create(dataset: CreateDataset, ctx: Request) -> DatasetResponse:
    """Endpoint to create a new dataset."""
    dataset_id = uuid4()
    mock_url = convert_mock_to_syftbox_path(dataset.mock_data_path, dataset_id)
    private_url = convert_private_to_syftbox_path(dataset.private_data_path, dataset_id)

    dataset = Dataset(
        id=dataset_id,
        name=dataset.name,
        description=dataset.description,
        tags=dataset.tags,
        private_data_path=private_url,
        mock_data_path=mock_url,
        created_at=str(datetime.now().isoformat()),
    )

    # Convert pydantic Model to json schema
    dataset_schema = dataset.model_json_schema()
    yaml_schema = yaml.dump(dataset_schema, default_flow_style=False)
    print(yaml_schema)

    logger.info(f"Got Create Dataset request - {dataset}")
    return DatasetResponse(msg="Dataset created successfully.")


def run_data_service():
    try:
        print("Running rpc server for", box.app_rpc_dir)
        box.run_forever()
    except Exception as e:
        print(e)
