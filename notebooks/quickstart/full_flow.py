# %%
import os

os.environ["SYFT_NO_REPR_HTML"] = "1"

from pathlib import Path

from syft_runtime.main import CodeRuntime


from syft_rds.orchestra import setup_rds_stack
from syft_rds import RDS_NOTEBOOKS_PATH


stack = setup_rds_stack(log_level="INFO")
do_client = stack.do_rds_client
ds_client = stack.ds_rds_client

CWD = RDS_NOTEBOOKS_PATH / "quickstart"


# Not really needed for function case
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
    name="sql_dataset",
    path=private_dir,  # we don't need this in function case
    mock_path=mock_dir,  # we don't need this in function case
    summary="dummy data",
    description_path=markdown_path,
)


# %%

query_path = CWD / "data" / "ds" / "query.sql"
os.makedirs(query_path.parent, exist_ok=True)
with open(query_path, "w") as f:
    f.write("select * from table")

ds_client.jobs.submit(
    user_code_path=query_path,
    dataset_name="sql_dataset",
)

# %%

job = do_client.jobs.get_all()[0]
# case 1: another binary
config = do_client.get_default_config_for_job(job)
config.runtime.cmd = ["wc"]
config.use_docker = False
do_client.run_private(job, config)

# %%
# case 2: custom Data-Owner python code

# store this as part of dataset?
script_path = Path("/Users/azizwork/Workspace/rds/notebooks/quickstart/data/do_code.py")
config.runtime = CodeRuntime(
    cmd=["python", script_path.as_posix()],
    cwd=script_path.parent.as_posix(),
)


config.use_docker = True
do_client.run_private(job, config)

# %%

# case 3: custom Data-Owner python code with wrapper


# @syftbox_private_function
# def custom_code(input_string):
#     print(len(input_string.split(" ")))
#     return len(input_string.split(" "))


# config.runner_args = custom_code.as_args()
# do_client.run_private(job, config)
