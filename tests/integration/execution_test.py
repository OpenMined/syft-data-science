import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, Job, JobStatus
from syft_rds.client.rds_clients.runtime import (
    DEFAULT_DOCKERFILE_FILE_PATH,
)

from tests.conftest import DS_PATH
from tests.utils import create_dataset
from loguru import logger
import time


single_file_submission = {"user_code_path": DS_PATH / "ds.py"}
folder_submission = {"user_code_path": DS_PATH / "code", "entrypoint": "main.py"}
runtime_configs = {
    "default_runtime": {},
    "docker_runtime": {
        "runtime_kind": "docker",
        "runtime_config": {"dockerfile": str(DEFAULT_DOCKERFILE_FILE_PATH)},
    },
    "python_runtime": {"runtime_kind": "python"},
    "named_python_runtime": {
        "runtime_name": "my_python_runtime",
        "runtime_kind": "python",
    },
}
test_cases = []
for sub_type, sub_params in [
    ("single_file", single_file_submission),
    ("folder", folder_submission),
]:
    for rt_name, rt_params in runtime_configs.items():
        for blocking_mode in [True, False]:
            blocking_str = "blocking" if blocking_mode else "non_blocking"
            test_cases.append(
                {
                    "id": f"{sub_type}_{rt_name}_{blocking_str}",
                    "submission_params": {**sub_params, **rt_params},
                    "expected_num_runtimes": 1,
                    "blocking": blocking_mode,
                }
            )


@pytest.mark.parametrize(
    "test_case",
    test_cases,
    ids=lambda x: x["id"],
)
def test_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    test_case: dict,
    monkeypatch,
):
    """Test job execution with a file or a folder, for various configurations."""
    blocking_mode = test_case["blocking"]
    monkeypatch.setenv("SYFT_RDS_BLOCKING_EXECUTION", str(blocking_mode).lower())
    submit_kwargs = {
        "dataset_name": "dummy",
        **test_case["submission_params"],
    }

    create_dataset(do_rds_client, "dummy")

    # DS submits job
    job = ds_rds_client.jobs.submit(**submit_kwargs)
    assert job.status == JobStatus.pending_code_review

    assert len(do_rds_client.runtime.get_all()) == test_case["expected_num_runtimes"]

    # DO reviews, runs, and shares job
    _run_and_verify_job(do_rds_client, blocking=blocking_mode)


def _run_and_verify_job(do_rds_client: RDSClient, blocking: bool):
    """Helper function to run a job and verify its execution."""
    # Server side: Get the job from the server.
    # We assume there is only one job in the queue.
    job: Job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side: Execute the job
    do_rds_client.run_private(job)

    if not blocking:
        assert job.status == JobStatus.job_in_progress

    if job.status == JobStatus.job_run_failed:
        logger.info(f"Job failed: {job.error_message}")
        raise Exception(f"Job failed: {job.error_message}")

    time.sleep(2)
    assert job.status == JobStatus.job_run_finished

    # DO shares results with DS
    do_rds_client.jobs.share_results(job)
    assert job.status == JobStatus.shared

    # DS checks for output
    output_path = job.get_output_path()
    assert output_path.exists()

    all_files_folders = list(output_path.glob("**/*"))
    all_files = [f for f in all_files_folders if f.is_file()]
    assert len(all_files) == 3  # output.txt, stdout.log, stderr.log

    my_result = output_path / "output" / "my_result.csv"
    assert my_result.exists()
    with open(my_result, "r") as f:
        assert f.read() == "Hello, world!"
