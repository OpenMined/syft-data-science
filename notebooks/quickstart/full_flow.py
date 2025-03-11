# %%
from syft_rds.orchestra import setup_rds_stack
from syft_rds import RDS_NOTEBOOKS_PATH

stack = setup_rds_stack(log_level="INFO")
do_client = stack.do_rds_client
ds_client = stack.ds_rds_client

CWD = RDS_NOTEBOOKS_PATH / "quickstart"


private_dir = CWD / "data" / "dataset-1" / "private"
mock_dir = CWD / "data" / "dataset-1" / "mock"
markdown_path = CWD / "data" / "dataset-1" / "description.md"

private_dir.mkdir(parents=True, exist_ok=True)
mock_dir.mkdir(parents=True, exist_ok=True)


with open(private_dir / "data.csv", "w") as f:
    f.write("-1,-2,-3")

with open(mock_dir / "data.csv", "w") as f:
    f.write("1,2,3")

with open(markdown_path, "w") as f:
    f.write("some description")

# %%
data = do_client.dataset.create(
    name="dataset-1",  # MUST BE UNIQUE. Throw Exception if already exist.
    path=private_dir,  # MUST EXIST
    mock_path=mock_dir,
    summary="dummy data",
    description_path=markdown_path,
)

# %%
