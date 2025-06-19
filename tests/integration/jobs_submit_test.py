import pytest

from tests.conftest import DS_PATH
from tests.utils import create_dataset

from syft_rds.client.rds_clients.runtime import (
    DEFAULT_RUNTIME_KIND,
    DEFAULT_RUNTIME_NAME,
    DEFAULT_DOCKERFILE_FILE_PATH,
)
from syft_rds.client.rds_client import RDSClient
from syft_rds.models import JobStatus


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
            DEFAULT_RUNTIME_KIND,
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

    # if "runtime_config" in submit_kwargs:
    #     # runtime.config is an object, so we convert it to a dict for comparison
    #     assert runtime.config.model_dump(mode="json") == submit_kwargs["runtime_config"]
