import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus
from syft_rds.client.rds_clients.runtime import (
    DEFAULT_DOCKERFILE_FILE_PATH,
)

from tests.conftest import DS_PATH
from tests.utils import create_dataset
from loguru import logger


def test_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
):
    user_code_path = DS_PATH / "ds.py"
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
    if job.status == JobStatus.job_run_failed:
        logger.error(f"Job failed: {job.error_message}")
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3


@pytest.mark.parametrize(
    "runtime_kind",
    [
        "docker",
        "python",
    ],
)
def test_job_folder_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    runtime_kind: str,
):
    # DO create dataset
    create_dataset(do_rds_client, "dummy")

    # DS submits job
    user_code_dir = DS_PATH / "code"
    entrypoint = "main.py"
    if runtime_kind == "docker":
        job = ds_rds_client.jobs.submit(
            dataset_name="dummy",
            user_code_path=user_code_dir,
            entrypoint=entrypoint,
            runtime_kind="docker",
            runtime_config={"dockerfile": str(DEFAULT_DOCKERFILE_FILE_PATH)},
        )
    else:
        job = ds_rds_client.jobs.submit(
            dataset_name="dummy",
            user_code_path=user_code_dir,
            entrypoint=entrypoint,
            runtime_kind="python",
        )

    assert job.status == JobStatus.pending_code_review
    assert len(do_rds_client.runtime.get_all()) == 1

    # DO reviews job
    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side (DO)
    do_rds_client.run_private(job)
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3  # output.txt, stdout.log, stderr.log

    output_txt = output_path / "output" / "output.txt"
    assert output_txt.exists()
    with open(output_txt, "r") as f:
        assert f.read() == "ABC"


def test_job_folder_execution_python_runtime(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
):
    create_dataset(do_rds_client, "dummy")
    job = ds_rds_client.jobs.submit(
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="dummy",
        runtime_name="my_python_runtime",
        runtime_kind="python",
    )
    assert job.status == JobStatus.pending_code_review

    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    do_rds_client.run_private(job)
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3

    output_txt = output_path / "output" / "output.txt"
    assert output_txt.exists()
    with open(output_txt, "r") as f:
        assert f.read() == "ABC"


def test_job_folder_execution_default_runtime(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
):
    create_dataset(do_rds_client, "dummy")
    job = ds_rds_client.jobs.submit(
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="dummy",
    )
    assert job.status == JobStatus.pending_code_review

    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    do_rds_client.run_private(job)
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3

    output_txt = output_path / "output" / "output.txt"
    assert output_txt.exists()
    with open(output_txt, "r") as f:
        assert f.read() == "ABC"
