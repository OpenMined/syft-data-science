import os

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import GetAllRequest, JobStatus
from syft_rds.client.rds_clients.runtime import (
    DEFAULT_RUNTIME_NAME,
    DEFAULT_DOCKERFILE_FILE_PATH,
)

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
    "submit_kwargs, expected_runtime_kind, expected_runtime_name",
    [
        # No custom runtime (default: Docker python)
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
            },
            "docker",
            DEFAULT_RUNTIME_NAME,
        ),
        # Python runtime
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
                "runtime_name": "python3.12",
                "runtime_kind": "python",
            },
            "python",
            "python3.12",
        ),
        # Docker with a Dockerfile (with custom name)
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
                "runtime_name": "my_docker_python",
                "runtime_kind": "docker",
                "runtime_config": {"dockerfile": str(DEFAULT_DOCKERFILE_FILE_PATH)},
            },
            "docker",
            "my_docker_python",
        ),
        # Docker with a Dockerfile (no custom name)
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
                "runtime_kind": "docker",
                "runtime_config": {"dockerfile": str(DEFAULT_DOCKERFILE_FILE_PATH)},
            },
            "docker",
            None,
        ),
        # Kubernetes with a pre-built image (no custom name)
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
                "runtime_kind": "kubernetes",
                "runtime_config": {
                    "image": "myregistry/myimage:latest",
                    "namespace": "research",
                    "num_workers": 3,
                },
            },
            "kubernetes",
            None,
        ),
        # Kubernetes with a pre-built image (with custom name)
        (
            {
                "name": "My FL Flower Experiment",
                "description": "Some description",
                "user_code_path": f"{DS_PATH / "code"}",
                "entrypoint": "main.py",
                "dataset_name": "dummy",
                "runtime_name": "my_k8s_runtime",
                "runtime_kind": "kubernetes",
                "runtime_config": {
                    "image": "myregistry/myimage:latest",
                    "namespace": "syft-rds",
                    "num_workers": 3,
                },
            },
            "kubernetes",
            "my_k8s_runtime",
        ),
    ],
)
def test_job_submit_with_custom_runtime(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    submit_kwargs,
    expected_runtime_kind,
    expected_runtime_name,
):
    # DO: create dataset
    create_dataset(do_rds_client, name=submit_kwargs["dataset_name"])

    # DS: submit job
    job = ds_rds_client.jobs.submit(**submit_kwargs)

    assert job is not None
    assert job.status == JobStatus.pending_code_review

    # DO: check runtime name and kind and config
    runtime = do_rds_client.runtime.get(uid=job.runtime_id)

    assert runtime.kind == expected_runtime_kind
    if expected_runtime_name:
        assert runtime.name == expected_runtime_name
    else:
        assert runtime.name.startswith(f"{expected_runtime_kind}_")

    if "runtime_config" in submit_kwargs:
        # runtime.config is an object, so we convert it to a dict for comparison
        assert runtime.config.model_dump(mode="json") == submit_kwargs["runtime_config"]


@pytest.mark.parametrize(
    "use_docker",
    [
        True,
        False,
    ],
)
def test_job_folder_execution(
    ds_rds_client: RDSClient,
    do_rds_client: RDSClient,
    use_docker: bool,
):
    # DO create dataset
    create_dataset(do_rds_client, "dummy")

    # DS submits job
    user_code_dir = DS_PATH / "code"
    entrypoint = "main.py"
    if use_docker:
        job = ds_rds_client.jobs.submit(
            dataset_name="dummy",
            user_code_path=user_code_dir,
            entrypoint=entrypoint,
            runtime_kind="docker",
            runtime_config={"dockerfile": str(DS_PATH / "Dockerfile")},
        )
    else:
        job = ds_rds_client.jobs.submit(
            dataset_name="dummy",
            user_code_path=user_code_dir,
            entrypoint=entrypoint,
        )
    assert job.status == JobStatus.pending_code_review

    # DO reviews job
    job = do_rds_client.rpc.jobs.get_all(GetAllRequest())[0]

    # Runner side (DO)
    config = do_rds_client.get_default_config_for_job(job)
    config.use_docker = use_docker

    from loguru import logger

    logger.info(f"Running job: {job.name}")
    logger.info(f"Job config: {config}")

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
