import os

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus, JobErrorKind
from tests.conftest import DS_PATH, PRIVATE_CODE_PATH
from tests.utils import create_dataset, create_dataset_with_custom_runtime


@pytest.mark.parametrize(
    "use_docker",
    [
        # True, # TODO setup docker flow in CI
        False,
    ],
)
def test_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    use_docker: bool,
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
    config = do_rds_client.get_default_config_for_job(job)
    config.use_docker = use_docker
    do_rds_client.run_private(job, config)
    assert job.status == JobStatus.job_run_finished

    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3


@pytest.mark.parametrize(
    "use_docker",
    [
        # True, # TODO setup docker flow in CI
        False,
    ],
)
def test_job_execution_timeout(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    use_docker: bool,
):
    user_code_path = DS_PATH / "ds_timeout.py"
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
    config = do_rds_client.get_default_config_for_job(job)
    config.use_docker = use_docker
    config.timeout = 3
    do_rds_client.run_private(job, config)
    assert job.status == JobStatus.job_run_failed
    assert job.error == JobErrorKind.timeout


@pytest.mark.parametrize(
    "use_docker",
    [
        # True, # TODO setup docker flow in CI
        False,
    ],
)
def test_job_folder_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    use_docker: bool,
):
    user_code_dir = DS_PATH / "code"
    entrypoint = "main.py"
    create_dataset(do_rds_client, "dummy")
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=user_code_dir,
        entrypoint=entrypoint,
        dataset_name="dummy",
    )
    assert job.status == JobStatus.pending_code_review

    # Server Side
    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side
    config = do_rds_client.get_default_config_for_job(job)
    config.use_docker = use_docker
    do_rds_client.run_private(job, config)
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


@pytest.mark.parametrize(
    "use_docker",
    [
        # True, # TODO setup docker flow in CI
        False,
    ],
)
def test_job_execution_with_custom_runtime(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    use_docker: bool,
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
    assert job.status == JobStatus.job_run_failed, "Need to set`SECRET_KEY`"

    os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
    config = do_rds_client.get_default_config_for_job(job)
    config.use_docker = use_docker
    if use_docker:
        config.runtime.mount_dir = PRIVATE_CODE_PATH
    config.extra_env["SECRET_KEY"] = "__AA__"
    do_rds_client.run_private(job, config)

    assert job.status == JobStatus.job_run_finished
    do_rds_client.jobs.share_results(job)
    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3
