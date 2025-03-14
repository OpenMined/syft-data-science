from pathlib import Path

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest
from tests.conftest import DS_PATH


def create_dataset(tmp_path: Path):
    private_dir = tmp_path / "private"
    mock_dir = tmp_path / "mock"
    markdown_path = tmp_path / "description.md"

    private_dir.mkdir(parents=True, exist_ok=True)
    mock_dir.mkdir(parents=True, exist_ok=True)

    with open(private_dir / "data.csv", "w") as f:
        f.write("-1,-2,-3")

    with open(mock_dir / "data.csv", "w") as f:
        f.write("1,2,3")

    with open(markdown_path, "w") as f:
        f.write("some description")
    return mock_dir, private_dir, markdown_path


@pytest.mark.parametrize(
    "user_code_path, runtime",
    [
        (DS_PATH / "ds.py", "python"),  # Python test case
        # (DS_PATH / "ds.sh", "bash"),  # Bash test case working on this
    ],
)
def test_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    user_code_path: Path,
    runtime: str,
    tmp_path: Path,
):
    mock_dir, private_dir, markdown_path = create_dataset(tmp_path)

    do_rds_client.dataset.create(
        name="dummy",
        path=private_dir,
        mock_path=mock_dir,
        description_path=markdown_path,
    )
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=user_code_path,
        dataset_name="dummy",
    )

    # Server Side
    jobs = do_rds_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    with pytest.raises(ValueError):
        # can't share artifacts before the job is run
        job.share_artifacts()

    # Runner side
    user_code = do_rds_client.user_code.get(job.user_code_id)
    assert user_code.local_dir.is_dir() and user_code.local_file.is_file()
    # config = JobConfig(
    #     # we use the parent directory of the user code path as the function folder
    #     # and the name of the user code file as the args.
    #     # the following commands are equivalent to each other:
    #     # $ cd dir && python main.py
    #     # $ cd job.user_code.path.parent && job.runtime job.user_code.path.name
    #     function_folder=user_code.local_dir,
    #     args=[user_code.file_name],
    #     data_path=PRIVATE_DATA_PATH,
    #     runtime=runtime,
    #     job_folder=DO_OUTPUT_PATH / str(job.name),
    #     timeout=1,
    #     use_docker=False,
    # )

    do_rds_client.run_private(job)

    do_rds_client.jobs.share_results(job)

    # check the output and logs

    job_output_folder = (
        do_rds_client.config.runner_config.job_output_folder / job.uid.hex
    )
    assert (job_output_folder / "output" / "my_result.csv").exists()
    assert (job_output_folder / "logs" / "stdout.log").exists()
    assert (job_output_folder / "logs" / "stderr.log").exists()

    # Only print this for the Python test case
    if runtime is None:
        print(ds_rds_client.local_store.jobs.store.db_path)
    print(ds_rds_client.local_store.jobs.store.db_path)

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files = list(output_path.glob("**/*"))
    assert len(all_files) > 0
    # make sure has some nested files
