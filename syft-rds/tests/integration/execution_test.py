from pathlib import Path
from typing import Callable, Type

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus


from syft_rds.store import YAMLFileSystemDatabase
from syft_runtime import (
    DockerRunner,
    FileOutputHandler,
    JobConfig,
    RichConsoleUI,
)


def test_job_execution(
    rds_client: RDSClient,
    server_client: RDSClient,
):
    # Client Side
    test_dir = Path(__file__).parent
    job = rds_client.jobs.submit(
        user_code_path=test_dir / "assets/ds/ds.py",
    )

    # Server Side
    jobs = server_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    job.add_to_queue()

    with pytest.raises(ValueError):
        # can't share artifacts before the job is run
        job.share_artifacts()

    # Runner side
    config = JobConfig(
        # we use the parent directory of the user code path as the function folder
        # and the name of the user code file as the args.
        # the following commands are equivalent to each other:
        # $ cd dir && python main.py
        # $ cd job.user_code.path.parent && job.runtime job.user_code.path.name
        function_folder=job.user_code.path.parent,
        args=[job.user_code.path.name],
        data_path=test_dir / "assets/do",
        runtime=job.runtime,
        job_folder=test_dir / "assets/do/job_outputs" / str(job.name),
    )

    runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])

    return_code = runner.run(
        config,
    )

    # Update job status based on return code
    job.status = (
        JobStatus.job_run_finished if return_code == 0 else JobStatus.job_run_failed
    )
    rds_client.rpc.jobs.update(job)

    job.share_artifacts()

    # check the output and logs
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "output" / "my_result.csv"
    ).exists()
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "logs" / "stdout.log"
    ).exists()
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "logs" / "stderr.log"
    ).exists()


def test_bash_job_execution(
    rds_client: RDSClient,
    server_client: RDSClient,
):
    # Client Side
    test_dir = Path(__file__).parent
    job = rds_client.jobs.submit(
        user_code_path=test_dir / "assets/ds/ds.sh",
    )

    # Server Side
    jobs = server_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    job.add_to_queue()

    with pytest.raises(ValueError):
        # can't share artifacts before the job is run
        job.share_artifacts()

    # Runner side
    config = JobConfig(
        # we use the parent directory of the user code path as the function folder
        # and the name of the user code file as the args.
        # the following commands are equivalent to each other:
        # $ cd dir && python main.py
        # $ cd job.user_code.path.parent && job.runtime job.user_code.path.name
        function_folder=job.user_code.path.parent,
        args=[job.user_code.path.name],
        data_path=test_dir / "assets/do",
        runtime="bash",
        job_folder=test_dir / "assets/do/job_outputs" / str(job.name),
    )

    runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])

    return_code = runner.run(
        config,
    )

    # Update job status based on return code
    job.status = (
        JobStatus.job_run_finished if return_code == 0 else JobStatus.job_run_failed
    )
    rds_client.rpc.jobs.update(job)

    job.share_artifacts()

    # check the output and logs
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "output" / "my_result.csv"
    ).exists()
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "logs" / "stdout.log"
    ).exists()
    assert (
        test_dir / "assets/do/job_outputs" / str(job.name) / "logs" / "stderr.log"
    ).exists()
