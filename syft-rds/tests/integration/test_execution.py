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


def test_job_execution(rds_client: RDSClient, server_client: RDSClient):
    # Client Side
    # job_create = JobCreate(
    #     name="Test Job", runtime="python3.9", user_code_id=uuid4(), tags=["test"]
    # )
    # job = rds_client.rpc.jobs.create(job_create)
    test_dir = Path(__file__).parent
    job = rds_client.jobs.submit(
        user_code_path=test_dir / "assets/ds/ds.py",
    )
    rds_client.rpc.jobs.update(job)
    # Server Side
    jobs = server_client.rpc.jobs.get_all(GetAllRequest())
    assert len(jobs) == 1
    assert jobs[0].uid == job.uid

    job = jobs[0]

    job.add_to_queue()

    with pytest.raises(ValueError):
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
        timeout=1,
        data_mount_dir="/data",
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
