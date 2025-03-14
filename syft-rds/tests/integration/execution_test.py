import os
from pathlib import Path

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus
from tests.conftest import DS_PATH
from tests.utils import create_dataset, create_dataset_with_custom_runtime


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
):
    create_dataset(do_rds_client, "dummy")
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=user_code_path,
        dataset_name="dummy",
    )
    assert job.status == JobStatus.pending_code_review

    # Server Side
    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side
    do_rds_client.run_private(job)
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3


def test_job_execution_with_custom_runtime(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
):
    create_dataset_with_custom_runtime(do_rds_client, "dummy")
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=DS_PATH / "ds.txt",
        dataset_name="dummy",
    )

    # Server Side
    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side
    do_rds_client.run_private(job)

    # do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.job_run_failed, "Need to set`SECRET_KEY`"

    os.environ["SECRET_KEY"] = "__AA__"
    do_rds_client.run_private(job)
    assert job.status == JobStatus.job_run_finished
    do_rds_client.jobs.share_results(job)
    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3
