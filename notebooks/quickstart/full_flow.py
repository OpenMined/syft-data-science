# %%
import os

os.environ["SYFT_NO_REPR_HTML"] = "1"

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
# %%


# case 1: another binary
config = do_client.get_default_config_for_job(job)
config.runner_args = ["wc"]
do_client.run_private(job, config)

# %%


# store this as part of dataset?
# how does it work with docker?
config.runner_args = [
    "python",
    "/Users/azizwork/Workspace/rds/notebooks/quickstart/data/do_code.py",
]

config.use_docker = True
do_client.run_private(job, config)

# %%

# case 3: custom Data-Owner python code with wrapper

from functools import wraps
import inspect
from pydantic import BaseModel
from pathlib import Path
import textwrap
from typing import Any


def syftbox_private_function(f):
    # Create a directory for storing the function if it doesn't exist
    os.makedirs("syftbox_functions", exist_ok=True)

    # Dump the function to a file
    function_name = f.__name__
    function_code = inspect.getsource(f)
    function_code_lines = function_code.split("\n")
    # TODO: FIX
    function_code_lines = function_code_lines[2:]
    function_code = "\n".join(function_code_lines)
    # dedent the function code
    function_code = textwrap.dedent(function_code)
    with open(f"syftbox_functions/{function_name}.py", "w") as file:
        file.write(function_code)

    # wraps the function and dumps the function to a file
    @wraps(f)
    def wrapper(*args, **kwargs):
        # Call the original function
        result = f(*args, **kwargs)
        return result

    return PrivateFunction(
        path=Path(f"syftbox_functions/{function_name}.py"), function=wrapper
    )


class PrivateFunction(BaseModel):
    path: Path
    function: Any

    def as_args(self):
        return ["python", self.path.as_posix()]

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)


@syftbox_private_function
def custom_code(input_string):
    print(len(input_string.split(" ")))
    return len(input_string.split(" "))


config.runner_args = custom_code.as_args()
do_client.run_private(job, config)

# %%
custom_code("hello world")
# %%
