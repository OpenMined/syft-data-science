from syft_rds.client.rds_client import RDSClient, connect
from syft_rds.services.dataset.dataset_model import CreateDataset

rds_client: RDSClient = connect("khoa@openmined.org")

dataset = CreateDataset(
    name="Census Dataset",
    description="Census Dataset for the year 1994",
    tags=["Census", "1994"],
    private_data_path="/home/dk/Desktop/projects/OpenMined/RDS/rds/notebooks/data/census/private_census.csv",
    mock_data_path="/home/dk/Desktop/projects/OpenMined/RDS/rds/notebooks/data/census/mock_census.csv",
)

future = rds_client.dataset.create(dataset)
print(f"Future: {future}")
print(f"Waiting for the response: {future.wait()}")


# client2: RDSClient = connect("yash@openmined.org")
# client2.dataset.create(dataset)
