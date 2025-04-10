from datetime import datetime
from uuid import UUID, uuid4

import pytest
from syft_rds.client.rds_client import RDSClient
from syft_rds.models.models import (
    GetAllRequest,
    GetOneRequest,
    Job,
    JobCreate,
    JobStatus,
    JobUpdate,
    RuntimeCreate,
    RuntimeUpdate,
    UserCodeCreate,
    UserCodeType,
)
from syft_rds.orchestra import RDSStack


def test_job_crud_file_rpc(rds_no_sync_stack: RDSStack):
    do_rds_client = rds_no_sync_stack.do_rds_client

    job_create = JobCreate(
        name="Test Job",
        runtime="python3.9",
        user_code_id=uuid4(),
        tags=["test"],
        dataset_name="test",
    )
    job = do_rds_client.rpc.jobs.create(job_create)
    assert job.name == "Test Job"

    # Get One
    get_req = GetOneRequest(uid=job.uid)
    fetched_job = do_rds_client.rpc.jobs.get_one(get_req)
    assert fetched_job == job

    # Insert second, get all
    job2_create = JobCreate(
        name="Test Job 2",
        runtime="python3.9",
        user_code_id=uuid4(),
        tags=["test"],
        dataset_name="test2",
    )
    job2 = do_rds_client.rpc.jobs.create(job2_create)

    all_req = GetAllRequest()
    all_jobs = do_rds_client.rpc.jobs.get_all(all_req)
    assert len(all_jobs) == 2

    for job in all_jobs:
        print(job)

    assert job in all_jobs
    assert job2 in all_jobs

    # partial update
    job_update = JobUpdate(uid=job.uid, status=JobStatus.rejected)
    job = do_rds_client.rpc.jobs.update(job_update)
    assert job.status == JobStatus.rejected


def test_job_crud(ds_rds_client: RDSClient, do_rds_client: RDSClient):
    job_create = JobCreate(
        name="Test Job",
        runtime="python3.9",
        user_code_id=uuid4(),
        tags=["test"],
        dataset_name="test",
    )
    job = ds_rds_client.rpc.jobs.create(job_create)
    assert job.name == "Test Job"

    # Get One
    get_req = GetOneRequest(uid=job.uid)
    fetched_job = ds_rds_client.rpc.jobs.get_one(get_req)
    assert fetched_job == job

    # Insert second, get all
    job2_create = JobCreate(
        name="Test Job 2",
        runtime="python3.9",
        user_code_id=uuid4(),
        tags=["test"],
        dataset_name="test2",
    )
    job2 = ds_rds_client.rpc.jobs.create(job2_create)

    all_req = GetAllRequest()
    all_jobs = ds_rds_client.rpc.jobs.get_all(all_req)
    assert len(all_jobs) == 2
    assert job in all_jobs
    assert job2 in all_jobs

    for job in all_jobs:
        print(job)

    # partial update
    job_update = JobUpdate(uid=job.uid, status=JobStatus.rejected)
    job = do_rds_client.rpc.jobs.update(job_update)
    assert job.status == JobStatus.rejected


def test_user_code_crud(ds_rds_client: RDSClient):
    user_code_create = UserCodeCreate(
        name="Test UserCode",
        code_type=UserCodeType.FILE,
        entrypoint="test.py",
    )
    user_code = ds_rds_client.rpc.user_code.create(user_code_create)
    assert user_code.name == "Test UserCode"

    # Get One
    get_req = GetOneRequest(uid=user_code.uid)
    fetched_code = ds_rds_client.rpc.user_code.get_one(get_req)
    assert fetched_code == user_code

    # Insert second, get all
    code2_create = UserCodeCreate(
        name="Test UserCode 2",
        code_type=UserCodeType.FILE,
        entrypoint="test2.py",
    )
    code2 = ds_rds_client.rpc.user_code.create(code2_create)

    all_req = GetAllRequest()
    all_codes = ds_rds_client.rpc.user_code.get_all(all_req)
    assert len(all_codes) == 2
    assert user_code in all_codes
    assert code2 in all_codes


def test_runtime_crud(ds_rds_client: RDSClient):
    runtime_create = RuntimeCreate(
        name="python3.9",
        description="Python 3.9 Runtime",
        tags=["python", "test"],
    )
    runtime = ds_rds_client.rpc.runtime.create(runtime_create)
    assert runtime.name == "python3.9"

    # Get One
    get_req = GetOneRequest(uid=runtime.uid)
    fetched_runtime = ds_rds_client.rpc.runtime.get_one(get_req)
    assert fetched_runtime == runtime

    # Insert second, get all
    runtime2_create = RuntimeCreate(
        name="python3.10",
        description="Python 3.10 Runtime",
        tags=["python", "test"],
    )
    runtime2 = ds_rds_client.rpc.runtime.create(runtime2_create)

    all_req = GetAllRequest()
    all_runtimes = ds_rds_client.rpc.runtime.get_all(all_req)
    assert len(all_runtimes) == 2
    assert runtime in all_runtimes
    assert runtime2 in all_runtimes


def test_apply_update(ds_rds_client: RDSClient):
    job = Job(
        dataset_name="test",
        user_code_id=uuid4(),
    )

    # apply job update
    job_update = JobUpdate(uid=job.uid, status=JobStatus.rejected)
    job.apply_update(job_update)
    assert job.status == JobStatus.rejected

    # apply job model
    new_job = job.model_copy()
    new_job.status = JobStatus.shared

    assert job.status != new_job.status
    job.apply_update(new_job)
    assert job.status == JobStatus.shared

    # Cannot apply update with different UID
    other_job_update = JobUpdate(uid=uuid4(), status=JobStatus.rejected)
    with pytest.raises(ValueError) as e:
        job.apply_update(other_job_update)
    print(e.exconly())

    # Cannot apply job with different uid
    other_job = Job(
        dataset_name="test",
        user_code_id=uuid4(),
    )

    with pytest.raises(ValueError) as e:
        job.apply_update(other_job)
    print(e.exconly())

    # Cannot apply non-job update to job
    with pytest.raises(ValueError) as e:
        job.apply_update(RuntimeUpdate(uid=job.uid))
    print(e.exconly())

    # Update in_place=False
    job.status = JobStatus.rejected
    job_update = JobUpdate(uid=job.uid, status=JobStatus.rejected)
    new_job = job.apply_update(job_update, in_place=False)

    assert job.status == JobStatus.rejected
    assert new_job.status == JobStatus.rejected


def test_search_with_filters(do_rds_client):
    # Create 10 sample jobs
    for i in range(10):
        job_create = JobCreate(
            name=f"Job {i}",
            runtime="python3.9",
            user_code_id=uuid4(),
            dataset_name="test",
        )
        do_rds_client.rpc.jobs.create(job_create)

    # Test successful coercion cases
    test_uuid = uuid4()
    coerced_filters = do_rds_client.local_store.jobs.store._coerce_field_types(
        {
            "status": "pending_code_review",
            "created_at": "2025-03-07T15:10:40.146495+00:00",
            "uid": test_uuid.hex,
        }
    )

    # Verify successful coercions
    assert isinstance(coerced_filters["status"], JobStatus)
    assert coerced_filters["status"] == JobStatus.pending_code_review
    assert isinstance(coerced_filters["created_at"], datetime)
    assert isinstance(coerced_filters["uid"], UUID)
    assert coerced_filters["uid"] == test_uuid

    # Test failed coercion cases - should return original values
    invalid_filters = do_rds_client.local_store.jobs.store._coerce_field_types(
        {
            "status": 1234,  # Not a valid enum string
            "created_at": "invalid-date",  # Not a valid date string
            "uid": "not-a-uuid",  # Not a valid UUID format
            "unknown_field": "some value",  # Field not in schema
        }
    )

    # Verify failed coercions returned original values
    assert invalid_filters["status"] == 1234
    assert invalid_filters["created_at"] == "invalid-date"
    assert invalid_filters["uid"] == "not-a-uuid"
    assert invalid_filters["unknown_field"] == "some value"

    # Test search using coerced enum works
    jobs = do_rds_client.jobs.get_all(status="pending_code_review")
    assert all(job.status == JobStatus.pending_code_review for job in jobs)

    # Test search with non-coercible value
    # This should work fine since store is schemaless
    jobs_with_invalid = do_rds_client.jobs.get_all(status=1234)
    assert len(jobs_with_invalid) == 0  # Assuming no job has status=1234
