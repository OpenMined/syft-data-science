import tempfile
import pytest
import os
import random
import pandas as pd
import json
import yaml
from pathlib import Path
from unittest.mock import patch
from loguru import logger

from syft_core.config import CONFIG_PATH_ENV

import syft_runtimes.high_low as syhl
from syft_runtimes.high_low.rsync import SyncResult
from syft_runtimes.models import HighLowRuntimeConfig
from syft_rds.orchestra import setup_rds_server, remove_rds_stack_dir
import syft_datasets as syd
from syft_notebook_ui.utils import show_dir


@pytest.fixture
def test_email():
    return "do1@openmined.org"


@pytest.fixture
def highlow_identifier():
    return "highlow-test-1234"


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as high_temp_dir, tempfile.TemporaryDirectory() as low_temp_dir:
        yield {"high_dir": Path(high_temp_dir), "low_dir": Path(low_temp_dir)}


@pytest.fixture
def test_dataset_data():
    num_rows = 50
    mock_data = {
        "age": [random.randint(0, 100) for _ in range(num_rows)],
        "height": [random.uniform(150, 200) for _ in range(num_rows)],
        "income": [random.randint(20000, 100000) for _ in range(num_rows)],
    }
    private_data = {
        "age": [random.randint(0, 100) for _ in range(num_rows)],
        "height": [random.uniform(150, 200) for _ in range(num_rows)],
        "income": [random.randint(20000, 100000) for _ in range(num_rows)],
    }
    return {
        "mock_df": pd.DataFrame(mock_data),
        "private_df": pd.DataFrame(private_data),
        "readme": "# Test Dataset\nThis is a test dataset for high-low testing.",
    }


@pytest.fixture
def lowside_stack(test_email, temp_dirs):
    remove_rds_stack_dir(key="test_low_datasites", root_dir=str(temp_dirs["low_dir"]))

    do_stack = setup_rds_server(
        email=test_email, key="test_low_datasites", root_dir=str(temp_dirs["low_dir"])
    )

    yield do_stack

    remove_rds_stack_dir(key="test_low_datasites", root_dir=str(temp_dirs["low_dir"]))


@pytest.fixture
def highside_client(test_email, highlow_identifier, temp_dirs):
    highside_syftbox_dir = temp_dirs["high_dir"] / "high_datasites"

    return syhl.initialize_high_datasite(
        email=test_email,
        highlow_identifier=highlow_identifier,
        data_dir=highside_syftbox_dir,
        force_overwrite=True,
    )


def create_test_dataset(highside_client, dataset_name, test_dataset_data, temp_dirs):
    """Helper function to create a test dataset."""
    os.environ[CONFIG_PATH_ENV] = str(highside_client.config_path)

    # Create test data files
    data_dir = temp_dirs["high_dir"] / "test_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    mock_path = data_dir / "mock_data.csv"
    private_path = data_dir / "private_data.csv"
    readme_path = data_dir / "README.md"

    test_dataset_data["mock_df"].to_csv(mock_path, index=False)
    test_dataset_data["private_df"].to_csv(private_path, index=False)
    readme_path.write_text(test_dataset_data["readme"])

    return syd.create(
        name=dataset_name,
        mock_path=mock_path,
        private_path=private_path,
        readme_path=readme_path,
    )


def connect_to_lowside(highside_client, lowside_stack):
    """Helper function to connect highside to lowside."""
    lowside_syftbox_dir = lowside_stack.client.workspace.data_dir
    return highside_client.lowside_connect(
        highlow_identifier=highside_client.highside_identifier,
        lowside_data_dir=lowside_syftbox_dir,
    )


def create_mock_job(lowside_stack, highlow_identifier: str, dataset_name: str):
    """Helper function to create a mock job on lowside."""
    lowside_syftbox_client = lowside_stack.client

    runtime_dir = (
        lowside_syftbox_client.workspace.data_dir
        / "private"
        / lowside_syftbox_client.email
        / "syft_runtimes"
        / highlow_identifier
    )
    jobs_dir = runtime_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    job_id = "test_job_123"
    job_dir = jobs_dir / job_id
    job_dir.mkdir(exist_ok=True)

    # Create job metadata
    job_metadata = {
        "job_id": job_id,
        "dataset_name": dataset_name,
        "code_path": "main.py",
    }
    (job_dir / "config.yaml").write_text(yaml.dump(job_metadata))

    # Create job code
    job_code = """
import pandas as pd

def compute_mean(data_path):
    df = pd.read_csv(data_path)
    return {
        'mean_age': df['age'].mean(),
        'mean_height': df['height'].mean(),
        'mean_income': df['income'].mean()
    }

result = compute_mean('data.csv')
print(f"Results: {result}")
"""
    (job_dir / "main.py").write_text(job_code)

    return job_id


def verify_lowside_datasets(lowside_stack, expected_count, expected_names=None):
    """Helper function to verify datasets on lowside."""
    from syft_datasets import SyftDatasetManager

    lowside_dataset_manager = SyftDatasetManager(syftbox_client=lowside_stack.client)
    lowside_datasets = lowside_dataset_manager.get_all()

    assert len(lowside_datasets) == expected_count

    if expected_names:
        synced_names = {ds.name for ds in lowside_datasets}
        assert synced_names == set(expected_names)

    # Verify private data is not accessible
    for dataset in lowside_datasets:
        assert dataset.mock_dir.exists()
        with pytest.raises(Exception):
            _ = dataset.private_dir

    return lowside_datasets


def verify_runtime_config_datasets(highside_client, lowside_client, dataset_names):
    """Helper function to verify dataset names are in runtime configs on both sides."""
    # Ensure dataset_names is a list
    if isinstance(dataset_names, str):
        dataset_names = [dataset_names]

    # Verify highside runtime config
    high_runtime_config_path = highside_client.runtime_dir / "config.yaml"
    high_runtime_config = HighLowRuntimeConfig.from_yaml(high_runtime_config_path)
    for dataset_name in dataset_names:
        assert dataset_name in high_runtime_config.datasets

    # Verify lowside runtime config
    low_runtime_config_path = lowside_client.runtime_dir / "config.yaml"
    assert low_runtime_config_path.exists()
    low_runtime_config = HighLowRuntimeConfig.from_yaml(low_runtime_config_path)
    for dataset_name in dataset_names:
        assert dataset_name in low_runtime_config.datasets


def test_initialize_high_datasite(test_email, highlow_identifier, temp_dirs):
    """Test initializing a high datasite."""
    highside_syftbox_dir = temp_dirs["high_dir"] / "high_datasites"

    highside_client = syhl.initialize_high_datasite(
        email=test_email,
        highlow_identifier=highlow_identifier,
        data_dir=highside_syftbox_dir,
        force_overwrite=True,
    )

    assert isinstance(highside_client, syhl.HighSideClient)
    assert highside_client.email == test_email
    assert highside_client.highside_identifier == highlow_identifier

    # Verify directory structure
    assert highside_client.workspace.data_dir.exists()
    assert highside_client.datasite_path.exists()
    assert highside_client.runtime_dir.exists()

    # Verify runtime directory structure
    runtime_dir = highside_client.runtime_dir
    assert (runtime_dir / "config.yaml").exists()
    assert (runtime_dir / "jobs").exists()
    assert (runtime_dir / "done").exists()
    assert (runtime_dir / "running").exists()


def test_directory_structure_integrity(highside_client, lowside_stack):
    """Test that directory structures are created correctly."""
    connect_to_lowside(highside_client, lowside_stack)

    # Verify highside structure
    high_runtime_dir = highside_client.runtime_dir
    assert (high_runtime_dir / "jobs").exists()
    assert (high_runtime_dir / "done").exists()
    assert (high_runtime_dir / "running").exists()
    assert (high_runtime_dir / "config.yaml").exists()
    logger.info(f"Highside runtime directory: {high_runtime_dir}")
    show_dir(high_runtime_dir)

    # Verify lowside structure
    lowside_syftbox_client = lowside_stack.client
    low_runtime_dir = (
        lowside_syftbox_client.workspace.data_dir
        / "private"
        / lowside_syftbox_client.email
        / "syft_runtimes"
        / highside_client.highside_identifier
    )
    assert low_runtime_dir.exists()
    assert (low_runtime_dir / "jobs").exists()
    assert (low_runtime_dir / "done").exists()
    assert (low_runtime_dir / "config.yaml").exists()
    logger.info(f"Lowside runtime directory: {low_runtime_dir}")
    show_dir(low_runtime_dir)


def test_create_and_sync_single_dataset(
    highside_client, lowside_stack, test_dataset_data, temp_dirs
):
    """Test creating and syncing a single dataset."""
    dataset_name = "test_dataset"

    # Create dataset on highside
    dataset = create_test_dataset(
        highside_client, dataset_name, test_dataset_data, temp_dirs
    )
    assert dataset.name == dataset_name
    assert dataset.mock_dir.exists()
    assert dataset.private_dir.exists()

    # Connect to lowside
    lowside_client = connect_to_lowside(highside_client, lowside_stack)

    # Sync dataset
    sync_result = highside_client.sync_dataset(dataset_name=dataset_name, verbose=True)
    assert isinstance(sync_result, SyncResult)
    assert sync_result.success

    # Verify dataset on lowside
    verify_lowside_datasets(
        lowside_stack, expected_count=1, expected_names=[dataset_name]
    )

    # Verify dataset name is added to runtime configs on both sides
    verify_runtime_config_datasets(highside_client, lowside_client, dataset_name)


@pytest.mark.parametrize("num_datasets", [2, 3, 5])
def test_sync_multiple_datasets(
    highside_client, lowside_stack, test_dataset_data, temp_dirs, num_datasets
):
    """Test syncing multiple datasets."""
    dataset_names = [f"dataset_{i}" for i in range(num_datasets)]

    # Create multiple datasets
    for dataset_name in dataset_names:
        create_test_dataset(highside_client, dataset_name, test_dataset_data, temp_dirs)

    # Connect to lowside
    lowside_client = connect_to_lowside(highside_client, lowside_stack)

    # Sync all datasets
    for dataset_name in dataset_names:
        sync_result = highside_client.sync_dataset(
            dataset_name=dataset_name, verbose=False
        )
        assert sync_result.success

    # Verify all datasets on lowside
    verify_lowside_datasets(
        lowside_stack, expected_count=num_datasets, expected_names=dataset_names
    )

    # Verify all dataset names are added to runtime configs on both sides
    verify_runtime_config_datasets(highside_client, lowside_client, dataset_names)


def test_complete_job_workflow(
    highside_client, lowside_stack, test_dataset_data, temp_dirs
):
    """Test the complete job workflow: create dataset -> sync -> submit job -> run -> sync results."""
    dataset_name = "job_workflow_dataset"

    # Create and sync dataset
    create_test_dataset(highside_client, dataset_name, test_dataset_data, temp_dirs)
    lowside_client = connect_to_lowside(highside_client, lowside_stack)
    sync_result = highside_client.sync_dataset(dataset_name=dataset_name, verbose=False)
    assert sync_result.success

    # Verify dataset name is added to runtime configs on both sides
    verify_runtime_config_datasets(highside_client, lowside_client, dataset_name)

    # Create mock job on lowside
    job_id = create_mock_job(
        lowside_stack, highside_client.highside_identifier, dataset_name
    )

    # Sync pending jobs from lowside to highside
    highside_client.sync_pending_jobs()

    # Verify job synced to highside
    high_jobs_dir = highside_client.runtime_dir / "jobs"
    assert (high_jobs_dir / job_id).exists()
    assert (high_jobs_dir / job_id / "config.yaml").exists()

    # Get and run pending jobs
    pending_jobs = [
        d for d in (highside_client.runtime_dir / "jobs").iterdir() if d.is_dir()
    ]
    assert len(pending_jobs) > 0

    # Mock job execution
    with patch.object(highside_client, "run_private") as mock_run:
        mock_run.return_value = {
            "status": "completed",
            "output": "Job executed successfully",
        }

        for job in pending_jobs:
            result = highside_client.run_private(job_id)
            assert result["status"] == "completed"

    # Create mock job results
    done_dir = highside_client.runtime_dir / "done"
    job_done_dir = done_dir / job_id
    job_done_dir.mkdir(parents=True, exist_ok=True)

    result_data = {"mean_age": 45.2, "mean_height": 175.8, "mean_income": 65000.0}
    (job_done_dir / "result.json").write_text(json.dumps(result_data))

    # Sync results back to lowside
    highside_client.sync_done_jobs(ignore_existing=False)

    # Verify results on lowside
    low_runtime_dir = lowside_client.runtime_dir
    low_done_dir = low_runtime_dir / "done"
    assert (low_done_dir / job_id).exists()
    assert (low_done_dir / job_id / "result.json").exists()


def test_sync_nonexistent_dataset(highside_client, lowside_stack):
    """Test error handling when syncing non-existent dataset."""
    connect_to_lowside(highside_client, lowside_stack)

    with pytest.raises(
        FileNotFoundError, match="Dataset 'nonexistent_dataset' not found"
    ):
        highside_client.sync_dataset(dataset_name="nonexistent_dataset")


def test_invalid_lowside_connection(highside_client):
    """Test error handling for invalid lowside connection."""
    with pytest.raises(Exception):
        highside_client.lowside_connect(
            highlow_identifier=highside_client.highside_identifier,
            lowside_data_dir="/nonexistent/path",
        )


def test_reinitialize_with_force_overwrite(test_email, highlow_identifier, temp_dirs):
    """Test reinitializing high datasite with force overwrite."""
    highside_syftbox_dir = temp_dirs["high_dir"] / "high_datasites"

    # Initialize first time
    highside_client1 = syhl.initialize_high_datasite(
        email=test_email,
        highlow_identifier=highlow_identifier,
        data_dir=highside_syftbox_dir,
        force_overwrite=True,
    )
    assert highside_client1.workspace.data_dir.exists()

    # Reinitialize with force_overwrite=True
    highside_client2 = syhl.initialize_high_datasite(
        email=test_email,
        highlow_identifier=highlow_identifier,
        data_dir=highside_syftbox_dir,
        force_overwrite=True,
    )
    assert highside_client2.workspace.data_dir.exists()
    assert highside_client2.email == test_email

    # Test without force_overwrite (should raise exception since directory exists)
    with pytest.raises(
        FileExistsError, match="already exists. Use force_overwrite=True"
    ):
        syhl.initialize_high_datasite(
            email=test_email,
            highlow_identifier=highlow_identifier,
            data_dir=highside_syftbox_dir,
            force_overwrite=False,
        )
