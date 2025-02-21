from loguru import logger

from syft_rds.client.rds_client import RDSClient, connect
from syft_rds.services.dataset.dataset_models import CreateDataset

rds_client: RDSClient = connect("khoa@openmined.org")

dataset = CreateDataset(
    name="Census Dataset",
    description="Census Dataset for the year 1994",
    tags=["Census", "1994"],
    private_data_path="./data/census/private_census.csv",
    mock_data_path="./data/census/mock_census.csv",
)

future = rds_client.dataset.create(dataset)

logger.debug(f"future = {future}")
try:
    logger.debug(f"Waiting for the response: {future.wait(timeout=2)}")
except Exception as e:
    logger.error(e)

# res = rds_client.dataset.get(future.id)
# print(res)
