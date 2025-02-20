from pathlib import Path
from uuid import uuid4

import pytest
from syft_core import Client, SyftClientConfig
from syft_event import SyftEvents
from syft_rds.client.rds_client import RDSClient, init_session
from syft_rds.models.models import (
    DatasetCreate,
    GetAllRequest,
    GetOneRequest,
    JobCreate,
    RuntimeCreate,
    UserCodeCreate,
)
from syft_rds.server.app import create_app

HOST_EMAIL = "alice@openmined.org"


@pytest.fixture
def host_syftbox_client(tmp_path):
    return Client(
        SyftClientConfig(
            email=HOST_EMAIL,
            client_url="http://localhost:5000",
            path=tmp_path / "syftbox_client_config.json",
        ),
    )


@pytest.fixture
def rds_server(host_syftbox_client: Client):
    return create_app(host_syftbox_client)


@pytest.fixture
def rds_client(rds_server: SyftEvents) -> RDSClient:
    return init_session(HOST_EMAIL, mock_server=rds_server)


def test_job_crud(rds_client: RDSClient):
    job_create = JobCreate(
        name="Test Job", runtime="python3.9", user_code_id=uuid4(), tags=["test"]
    )
    job = rds_client.rpc.jobs.create(job_create)
    assert job.name == "Test Job"

    # Get One
    get_req = GetOneRequest(uid=job.uid)
    fetched_job = rds_client.rpc.jobs.get_one(get_req)
    assert fetched_job == job

    # Insert second, get all
    job2_create = JobCreate(
        name="Test Job 2", runtime="python3.9", user_code_id=uuid4(), tags=["test"]
    )
    job2 = rds_client.rpc.jobs.create(job2_create)

    all_req = GetAllRequest()
    all_jobs = rds_client.rpc.jobs.get_all(all_req)
    assert len(all_jobs) == 2
    assert job in all_jobs
    assert job2 in all_jobs


def test_user_code_crud(rds_client: RDSClient):
    user_code_create = UserCodeCreate(
        name="Test UserCode",
        path=Path("~/test.py"),
        dataset_id=uuid4(),
    )
    user_code = rds_client.rpc.user_code.create(user_code_create)
    assert user_code.name == "Test UserCode"

    # Get One
    get_req = GetOneRequest(uid=user_code.uid)
    fetched_code = rds_client.rpc.user_code.get_one(get_req)
    assert fetched_code == user_code

    # Insert second, get all
    code2_create = UserCodeCreate(
        name="Test UserCode 2",
        path=Path("~/test2.py"),
        dataset_id=uuid4(),
    )
    code2 = rds_client.rpc.user_code.create(code2_create)

    all_req = GetAllRequest()
    all_codes = rds_client.rpc.user_code.get_all(all_req)
    assert len(all_codes) == 2
    assert user_code in all_codes
    assert code2 in all_codes


def test_runtime_crud(rds_client: RDSClient):
    runtime_create = RuntimeCreate(
        name="python3.9",
        description="Python 3.9 Runtime",
        tags=["python", "test"],
    )
    runtime = rds_client.rpc.runtime.create(runtime_create)
    assert runtime.name == "python3.9"

    # Get One
    get_req = GetOneRequest(uid=runtime.uid)
    fetched_runtime = rds_client.rpc.runtime.get_one(get_req)
    assert fetched_runtime == runtime

    # Insert second, get all
    runtime2_create = RuntimeCreate(
        name="python3.10",
        description="Python 3.10 Runtime",
        tags=["python", "test"],
    )
    runtime2 = rds_client.rpc.runtime.create(runtime2_create)

    all_req = GetAllRequest()
    all_runtimes = rds_client.rpc.runtime.get_all(all_req)
    assert len(all_runtimes) == 2
    assert runtime in all_runtimes
    assert runtime2 in all_runtimes
