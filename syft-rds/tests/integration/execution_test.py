from pathlib import Path
import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus
from syft_runtime import (
    DockerRunner,
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
)
from tests.conftest import DS_PATH, DO_OUTPUT_PATH, PRIVATE_DATA_PATH, MOCK_DATA_PATH


@pytest.mark.parametrize(
    "user_code_path, runtime",
    [
        (DS_PATH / "ds.py", "python"),  # Python test case
        (DS_PATH / "ds.sh", "bash"),  # Bash test case
    ],
)
def test_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    user_code_path: Path,
    runtime: str,
):
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=user_code_path,
        runtime=runtime,
        dataset_name="dummy",
    )

    # Server Side
    jobs = do_rds_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    job.add_to_queue()

    with pytest.raises(ValueError):
        # can't share artifacts before the job is run
        job.share_artifacts()

    # Runner side
    user_code = do_rds_client.user_code.get(job.user_code_id)
    config = JobConfig(
        # we use the parent directory of the user code path as the function folder
        # and the name of the user code file as the args.
        # the following commands are equivalent to each other:
        # $ cd dir && python main.py
        # $ cd job.user_code.path.parent && job.runtime job.user_code.path.name
        function_folder=user_code.path.parent,
        args=[user_code.path.name],
        data_path=PRIVATE_DATA_PATH,
        runtime=job.runtime,
        job_folder=DO_OUTPUT_PATH / str(job.name),
        timeout=1,
        use_docker=False,
    )

    runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])

    return_code = runner.run(config)
    assert return_code == 0

    # Update job status based on return code
    job.status = (
        JobStatus.job_run_finished if return_code == 0 else JobStatus.job_run_failed
    )
    ds_rds_client.rpc.jobs.update(job)

    job.share_artifacts()

    # check the output and logs
    assert (DO_OUTPUT_PATH / str(job.name) / "output" / "my_result.csv").exists()
    assert (DO_OUTPUT_PATH / str(job.name) / "logs" / "stdout.log").exists()
    assert (DO_OUTPUT_PATH / str(job.name) / "logs" / "stderr.log").exists()

    # Only print this for the Python test case
    if runtime is None:
        print(ds_rds_client.local_store.jobs.store.db_path)
    print(ds_rds_client.local_store.jobs.store.db_path)


def test_bash_job_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
):
    # Client Side
    job = ds_rds_client.jobs.submit(
        user_code_path=DS_PATH / "ds.sh",
        dataset_name="dummy",
    )

    # Server Side
    jobs = do_rds_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    job.add_to_queue()

    with pytest.raises(ValueError):
        # can't share artifacts before the job is run
        job.share_artifacts()

    user_code = do_rds_client.user_code.get(job.user_code_id)
    # Runner side
    config = JobConfig(
        # we use the parent directory of the user code path as the function folder
        # and the name of the user code file as the args.
        # the following commands are equivalent to each other:
        # $ cd dir && python main.py
        # $ cd job.user_code.path.parent && job.runtime job.user_code.path.name
        function_folder=user_code.path.parent,
        args=[user_code.path.name],
        data_path=MOCK_DATA_PATH,
        runtime="bash",
        job_folder=DO_OUTPUT_PATH / str(job.name),
    )

    runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])

    return_code = runner.run(config)

    # Update job status based on return code
    job.status = (
        JobStatus.job_run_finished if return_code == 0 else JobStatus.job_run_failed
    )
    ds_rds_client.rpc.jobs.update(job)

    job.share_artifacts()

    # check the output and logs
    assert (DO_OUTPUT_PATH / str(job.name) / "output" / "my_result.csv").exists()
    assert (DO_OUTPUT_PATH / str(job.name) / "logs" / "stdout.log").exists()
    assert (DO_OUTPUT_PATH / str(job.name) / "logs" / "stderr.log").exists()
