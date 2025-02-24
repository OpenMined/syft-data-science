from pathlib import Path

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest


from syft_runtime.syft_runtime.main import (
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
    job = rds_client.jobs.submit(
        name="Test Job",
        user_code_path=Path("syft-rds/tests/integration/assets/ds/ds.py"),
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
        data_path="syft-rds/tests/integration/assets/do/",
        runtime=job.runtime,
        job_folder=f"syft-rds/tests/integration/assets/do/job_outputs/{job.uid}",
        timeout=1,
        data_mount_dir="/data",
    )

    runner = DockerRunner(handlers=[FileOutputHandler(), RichConsoleUI()])

    runner.run(
        config,
    )

    job.share_artifacts()
