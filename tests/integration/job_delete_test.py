from uuid import uuid4

import pytest

from syft_rds.client.exceptions import RDSValidationError
from syft_rds.client.rds_client import RDSClient
from syft_rds.models import JobStatus
from tests.conftest import DS_PATH
from tests.utils import create_dataset


def test_job_delete_success(do_rds_client: RDSClient, ds_rds_client: RDSClient):
    """Test successful job deletion."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits job to DO
    job = ds_rds_client.job.submit(
        name="Test Job",
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="test_dataset",
    )

    # Verify job exists on DO side
    assert do_rds_client.job.get(uid=job.uid, mode="local") is not None

    # DO deletes the job
    result = do_rds_client.job.delete(job)
    assert result is True

    # Verify job no longer exists
    with pytest.raises(ValueError, match="No Job found"):
        do_rds_client.job.get(uid=job.uid, mode="local")


def test_job_delete_by_uuid(do_rds_client: RDSClient, ds_rds_client: RDSClient):
    """Test job deletion by UUID."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits job to DO
    job = ds_rds_client.job.submit(
        name="Test Job",
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="test_dataset",
    )

    job_uid = job.uid

    # DO deletes the job by UUID
    result = do_rds_client.job.delete(job_uid)
    assert result is True

    # Verify job no longer exists
    with pytest.raises(ValueError, match="No Job found"):
        do_rds_client.job.get(uid=job_uid, mode="local")


def test_job_delete_nonexistent(do_rds_client: RDSClient):
    """Test deletion of a non-existent job."""
    fake_uuid = uuid4()

    # Attempt to delete non-existent job
    result = do_rds_client.job.delete(fake_uuid)
    assert result is False


def test_job_delete_with_orphaned_usercode(
    do_rds_client: RDSClient, ds_rds_client: RDSClient
):
    """Test that orphaned UserCode is deleted when job is deleted."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits job to DO
    job = ds_rds_client.job.submit(
        name="Test Job",
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="test_dataset",
    )

    user_code_id = job.user_code_id

    # Verify UserCode exists on DO side
    usercode = do_rds_client.user_code.get(uid=user_code_id, mode="local")
    assert usercode is not None

    # DO deletes the job (should also delete orphaned UserCode)
    result = do_rds_client.job.delete(job, delete_orphaned_usercode=True)
    assert result is True

    # Verify UserCode was also deleted
    with pytest.raises(ValueError, match="No UserCode found"):
        do_rds_client.user_code.get(uid=user_code_id, mode="local")


def test_job_delete_preserve_shared_usercode(do_rds_client: RDSClient):
    """Test that shared UserCode is preserved when one job is deleted."""
    create_dataset(do_rds_client, "test_dataset")

    # Create UserCode first
    usercode = do_rds_client.user_code.create(
        code_path=DS_PATH / "code", entrypoint="main.py"
    )

    # Create runtime
    runtime = do_rds_client.runtime.create()

    # Submit two jobs using the same UserCode
    job1 = do_rds_client.job.create(
        name="Job 1",
        user_code=usercode,
        dataset_name="test_dataset",
        runtime=runtime,
    )

    job2 = do_rds_client.job.create(
        name="Job 2",
        user_code=usercode,
        dataset_name="test_dataset",
        runtime=runtime,
    )

    # Delete one job
    result = do_rds_client.job.delete(job1, delete_orphaned_usercode=True)
    assert result is True

    # Verify UserCode still exists (shared with job2)
    preserved_usercode = do_rds_client.user_code.get(uid=usercode.uid, mode="local")
    assert preserved_usercode is not None

    # Verify job2 still exists
    existing_job2 = do_rds_client.job.get(uid=job2.uid, mode="local")
    assert existing_job2 is not None


def test_job_delete_skip_usercode_cleanup(
    do_rds_client: RDSClient, ds_rds_client: RDSClient
):
    """Test job deletion without UserCode cleanup."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits job to DO
    job = ds_rds_client.job.submit(
        name="Test Job",
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="test_dataset",
    )

    user_code_id = job.user_code_id

    # DO deletes the job without UserCode cleanup
    result = do_rds_client.job.delete(job, delete_orphaned_usercode=False)
    assert result is True

    # Verify UserCode still exists on DO side
    usercode = do_rds_client.user_code.get(uid=user_code_id, mode="local")
    assert usercode is not None


def test_job_delete_all_no_filters(do_rds_client: RDSClient, ds_rds_client: RDSClient):
    """Test deleting all jobs without filters."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits multiple jobs to DO
    jobs = []
    for i in range(3):
        job = ds_rds_client.job.submit(
            name=f"Test Job {i}",
            user_code_path=DS_PATH / "code",
            entrypoint="main.py",
            dataset_name="test_dataset",
        )
        jobs.append(job)

    # Verify jobs exist on DO side
    all_jobs_before = do_rds_client.job.get_all(mode="local")
    assert len(all_jobs_before) >= 3

    # DO deletes all jobs
    deleted_count = do_rds_client.job.delete_all()

    # Verify correct number of jobs were deleted
    assert deleted_count >= 3

    # Verify no jobs remain
    all_jobs_after = do_rds_client.job.get_all(mode="local")
    assert len(all_jobs_after) == 0


def test_job_delete_all_with_status_filter(
    do_rds_client: RDSClient, ds_rds_client: RDSClient
):
    """Test deleting jobs with status filter."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits multiple jobs to DO
    jobs = []
    for i in range(3):
        job = ds_rds_client.job.submit(
            name=f"Test Job {i}",
            user_code_path=DS_PATH / "code",
            entrypoint="main.py",
            dataset_name="test_dataset",
        )
        jobs.append(job)

    # All jobs should have pending_code_review status by default
    # DO deletes only jobs with pending_code_review status
    deleted_count = do_rds_client.job.delete_all(status=JobStatus.pending_code_review)

    # Verify correct number of jobs were deleted
    assert deleted_count == 3

    # Verify no jobs with that status remain
    filtered_jobs = do_rds_client.job.get_all(
        mode="local", status=JobStatus.pending_code_review
    )
    assert len(filtered_jobs) == 0


def test_job_delete_all_empty_result(do_rds_client: RDSClient):
    """Test delete_all when no jobs match the filter."""
    # Ensure no jobs exist with rejected status
    deleted_count = do_rds_client.job.delete_all(status=JobStatus.rejected)

    # Should return 0
    assert deleted_count == 0


def test_job_delete_non_admin_fails(ds_rds_client: RDSClient):
    """Test that non-admin users cannot delete jobs."""
    fake_uuid = uuid4()

    # Non-admin user should not be able to delete jobs
    with pytest.raises(RDSValidationError, match="Only admins can delete jobs"):
        ds_rds_client.job.delete(fake_uuid)


def test_job_delete_all_non_admin_fails(ds_rds_client: RDSClient):
    """Test that non-admin users cannot delete all jobs."""
    # Non-admin user should not be able to delete jobs
    with pytest.raises(RDSValidationError, match="Only admins can delete jobs"):
        ds_rds_client.job.delete_all()


def test_job_delete_output_folders(do_rds_client: RDSClient, ds_rds_client: RDSClient):
    """Test that job output folders are properly deleted."""
    create_dataset(do_rds_client, "test_dataset")

    # DS submits job to DO
    job = ds_rds_client.job.submit(
        name="Test Job",
        user_code_path=DS_PATH / "code",
        entrypoint="main.py",
        dataset_name="test_dataset",
    )

    # Create mock output folders to simulate job execution
    if job.output_url:
        job_output_path = job.output_url.to_local_path(
            do_rds_client._syftbox_client.datasites
        )
        job_output_path.mkdir(parents=True, exist_ok=True)
        (job_output_path / "test_output.txt").write_text("test content")

    # Create runner output folder
    runner_output = do_rds_client.config.runner_config.job_output_folder / job.uid.hex
    runner_output.mkdir(parents=True, exist_ok=True)
    (runner_output / "runner_output.txt").write_text("runner content")

    # Verify folders exist
    if job.output_url:
        assert job_output_path.exists()
    assert runner_output.exists()

    # DO deletes the job
    result = do_rds_client.job.delete(job)
    assert result is True

    # Verify output folders were deleted
    if job.output_url:
        assert not job_output_path.exists()
    assert not runner_output.exists()
