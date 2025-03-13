# %%
import os

os.environ["SYFT_NO_REPR_HTML"] = "1"

from syft_rds.orchestra import setup_rds_stack
from syft_runtime.main import CodeRuntime
from syft_rds import RDS_NOTEBOOKS_PATH


stack = setup_rds_stack(log_level="INFO")
do_client = stack.do_rds_client
ds_client = stack.ds_rds_client

CWD = RDS_NOTEBOOKS_PATH / "quickstart"
markdown_path = CWD / "data" / "dataset-1" / "description.md"


private_code_path = CWD / "data" / "do" / "private_code"
public_code_path = CWD / "data" / "do" / "public_code"

data = do_client.dataset.create(
    name="sql_dataset",
    path=private_code_path,
    mock_path=public_code_path,
    summary="dummy data",
    description_path=markdown_path,
    runtime=CodeRuntime(
        cmd=["python", (private_code_path / "do_code.py").as_posix()],
        cwd=private_code_path.as_posix(),  # make it available to the container
        # image_name="python:3.11",
    ),
)
data

# %%

query_path = CWD / "data" / "ds" / "query.sql"

ds_client.jobs.submit(
    user_code_path=query_path,
    dataset_name="sql_dataset",
)

job = do_client.jobs.get_all()[0]
config = do_client.get_default_config_for_job(job)
config.use_docker = True
do_client.run_private(job, config)

# %%
