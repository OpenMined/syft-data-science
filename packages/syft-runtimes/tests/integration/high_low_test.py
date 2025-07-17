import tempfile
import pytest
from pathlib import Path
from syft_core import Client as SyftBoxClient
from syft_core import SyftClientConfig

import syft_runtimes.high_low as syhl
from syft_runtimes.high_low.rsync import RsyncConfig, Side
from syft_runtimes.high_low.setup import HighLowRuntimeConfig
from syft_datasets import SyftDatasetManager


DATA_DIR = Path(__file__).parent.parent / "assets" / "data"


@pytest.fixture
def temp_directories():
    """Create temporary directories for high and low sides."""
    with tempfile.TemporaryDirectory() as high_temp_dir, tempfile.TemporaryDirectory() as low_temp_dir:
        yield Path(high_temp_dir), Path(low_temp_dir)


@pytest.fixture
def lowside_client(temp_directories):
    """Create a low-side SyftBox client."""
    _, low_temp_dir = temp_directories

    # Create SyftBox config for low side
    low_config_path = low_temp_dir / "config.json"
    low_syftbox_dir = low_temp_dir / "SyftBox"

    config = SyftClientConfig(
        email="test@openmined.org",
        client_url="http://testserver:5000",
        path=low_config_path,
        data_dir=low_syftbox_dir,
    )
    config.save()

    client = SyftBoxClient(conf=config)
    client.datasite_path.mkdir(parents=True, exist_ok=True)
    return client


@pytest.fixture
def highside_client_and_config(lowside_client, temp_directories):
    """Create high-side client and sync configuration (that stays on the high side)."""
    high_temp_dir, _ = temp_directories
    highside_identifier = "test-highside-1234"

    # Initialize high datasite
    highside_data_dir = syhl.initialize_high_datasite(
        highside_identifier=highside_identifier,
        force_overwrite=True,
        highside_data_dir=high_temp_dir / "high_datasite",
        lowside_syftbox_client=lowside_client,
    )

    # Connect to high side
    highside_client = syhl.high_side_connect(
        email=lowside_client.email, data_dir=highside_data_dir
    )

    # Create sync configuration
    sync_config = syhl.create_default_sync_config(
        highside_client=highside_client,
        lowside_client=lowside_client,
        highside_identifier=highside_identifier,
        force_overwrite=True,
    )

    return highside_client, sync_config, highside_identifier


def test_high_runtime_dir_structure(highside_client_and_config):
    """Test that the runtime directory structure matches the expected layout from the folder picture."""
    highside_client, sync_config, highside_identifier = highside_client_and_config

    # Expected high datasite's structure:
    # SyftBox/private/{email}/syft_runtimes/{runtime_name}/
    #   ├── jobs/
    #   ├── running/
    #   ├── done/
    #   └── config.yaml

    # Test high side structure
    high_runtime_dir = sync_config.high_low_runtime_dir(Side.HIGH)
    expected_high_path = (
        highside_client.workspace.data_dir
        / "private"
        / highside_client.email
        / "syft_runtimes"
        / highside_identifier
    )

    assert high_runtime_dir == expected_high_path
    assert high_runtime_dir.exists()

    # Test required subdirectories
    required_subdirs = ["jobs", "running", "done"]
    for subdir in required_subdirs:
        assert (high_runtime_dir / subdir).exists()
        assert (high_runtime_dir / subdir).is_dir()

    # Test config.yaml exists and is readable
    config_file = high_runtime_dir / "config.yaml"
    assert config_file.exists()
    assert config_file.is_file()

    # Verify config content structure
    config = HighLowRuntimeConfig.from_yaml(config_file)
    assert hasattr(config, "datasets")
    assert hasattr(config, "config_path")
    assert config.config_path == config_file


def test_high_datasite_initialization(lowside_client, temp_directories):
    """Test that high datasite is initialized correctly with proper structure.
    high datasite:
    ├── config.json
    └── SyftBox
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-init
                        ├── config.yaml
                        ├── done
                        ├── jobs
                        └── running

    low datasite has the same structure:
    ├── config.json
    └── SyftBox
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-init
                        ├── config.yaml
                        ├── done
                        ├── jobs
                        └── running
    """
    high_temp_dir, _ = temp_directories
    highside_identifier = "test-highside-init"

    # Initialize high datasite
    highside_data_dir = syhl.initialize_high_datasite(
        highside_identifier=highside_identifier,
        force_overwrite=True,
        highside_data_dir=high_temp_dir / "high_datasite",
        lowside_syftbox_client=lowside_client,
    )

    # Verify directory structure
    assert highside_data_dir.exists()
    assert (highside_data_dir / "config.json").exists()
    assert (highside_data_dir / "SyftBox").exists()

    # Connect and verify client
    highside_client = syhl.high_side_connect(
        email=lowside_client.email, data_dir=highside_data_dir
    )

    assert highside_client.email == lowside_client.email
    assert highside_client.datasite_path.exists()


def test_sync_config_creation_and_runtime_structure(highside_client_and_config):
    """Test sync config creation and verify syft_runtimes structure.
    high datasite:
    ├── config.json
    └── SyftBox
        ├── high_side_sync_config.json  # this is where the sync config is saved on the high side
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-1234
                        ├── config.yaml
                        ├── done
                        ├── jobs
                        └── running
    low datasite:
    ├── config.json
    └── SyftBox
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-1234
                        ├── config.yaml
                        ├── done
                        ├── jobs
                        └── running
    """
    highside_client, sync_config, highside_identifier = highside_client_and_config

    # Verify sync config properties
    assert sync_config.high_side_name == highside_identifier
    assert sync_config.syftbox_client_email == highside_client.email
    assert len(sync_config.entries) == 2  # jobs and outputs sync entries

    # Verify high-side runtime directory structure
    high_runtime_dir = sync_config.high_low_runtime_dir(Side.HIGH)
    assert high_runtime_dir.exists()
    assert (high_runtime_dir / "jobs").exists()
    assert (high_runtime_dir / "running").exists()
    assert (high_runtime_dir / "done").exists()
    assert (high_runtime_dir / "config.yaml").exists()

    # Verify low-side runtime directory structure
    low_runtime_dir = sync_config.high_low_runtime_dir(Side.LOW)
    assert low_runtime_dir.exists()
    assert (low_runtime_dir / "jobs").exists()
    assert (low_runtime_dir / "running").exists()
    assert (low_runtime_dir / "done").exists()
    assert (low_runtime_dir / "config.yaml").exists()

    # Verify config file structure
    high_config = HighLowRuntimeConfig.from_yaml(high_runtime_dir / "config.yaml")
    assert high_config.config_path == high_runtime_dir / "config.yaml"
    assert isinstance(high_config.datasets, list)


def test_basic_sync_operations(highside_client_and_config):
    """Test basic sync operations for runtime folders.
    high datasite:
    ├── config.json
    └── SyftBox
        ├── high_side_sync_config.json  # this is where the sync config is saved on the high side
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-1234
                        ├── config.yaml
                        ├── done
                        │   └── job_000/  # this will be synced to the low side
                        │       └── config.yaml
                        │       ├── code/
                        │       ├── logs/
                        │       └── results/
                        ├── jobs
                        └── running
    low datasite:
    ├── config.json
    └── SyftBox
        ├── datasites
        │   └── test@openmined.org
        └── private
            └── test@openmined.org
                └── syft_runtimes
                    └── test-highside-1234
                        ├── config.yaml
                        ├── done
                        ├── jobs
                        │   └── job_001/  # this will be synced to the high side
                        │       ├── config.yaml
                        │       └── code/
                        │           └── analysis.py
                        └── running
    """
    highside_client, sync_config, _ = highside_client_and_config

    # Create a done job in the done directory on high side (simulating job completion)
    high_outputs_dir = sync_config.outputs_dir(Side.HIGH)
    test_done_job_dir = high_outputs_dir / "job_000"
    test_done_job_dir.mkdir(parents=True, exist_ok=True)
    (test_done_job_dir / "config.yaml").write_text(
        '{"uid": "test000abcd", "dataset_name": "test_dataset", "status": "completed"}'
    )
    (test_done_job_dir / "code").mkdir(parents=True, exist_ok=True)
    (test_done_job_dir / "code" / "analysis.py").write_text("print('Hello, world!')")
    (test_done_job_dir / "results").mkdir(parents=True, exist_ok=True)
    (test_done_job_dir / "results" / "results.csv").write_text("Hello, world!")
    (test_done_job_dir / "logs").mkdir(parents=True, exist_ok=True)
    (test_done_job_dir / "logs" / "stdout.log").write_text("Job completed successfully")

    # Create a job in jobs directory on low side (simulating job submission)
    low_jobs_dir = sync_config.jobs_dir(Side.LOW)
    test_new_job_dir = low_jobs_dir / "job_001"
    test_new_job_dir.mkdir(parents=True, exist_ok=True)
    (test_new_job_dir / "config.yaml").write_text(
        '{"uid": "test001abcd", "status": "pending_code_review"}'
    )
    (test_new_job_dir / "code").mkdir(parents=True, exist_ok=True)
    (test_new_job_dir / "code" / "analysis.py").write_text("print('Hello, world!')")

    # check that the job does not exist on the high side
    high_jobs_dir = sync_config.jobs_dir(Side.HIGH)
    assert not (high_jobs_dir / "job_001").exists()

    # check that the output does not exist on the low side
    low_outputs_dir = sync_config.outputs_dir(Side.LOW)
    assert not (low_outputs_dir / "job_000").exists()

    # Perform sync operation
    syhl.sync(syftbox_client=highside_client, rsync_config=sync_config)

    # Verify files synced correctly
    # Job should sync from low to high (REMOTE_TO_LOCAL)
    assert (high_jobs_dir / "job_001").exists()
    assert (high_jobs_dir / "job_001" / "config.yaml").exists()
    assert (high_jobs_dir / "job_001" / "code" / "analysis.py").exists()

    # Output should sync from high to low (LOCAL_TO_REMOTE)
    assert (low_outputs_dir / "job_000").exists()
    assert (low_outputs_dir / "job_000" / "config.yaml").exists()
    assert (low_outputs_dir / "job_000" / "code" / "analysis.py").exists()
    assert (low_outputs_dir / "job_000" / "results" / "results.csv").exists()
    assert (low_outputs_dir / "job_000" / "logs" / "stdout.log").exists()


def test_dataset_creation_and_sync(highside_client_and_config, lowside_client):
    """Test dataset creation on high side and syncing to low side."""
    highside_client, sync_config, _ = highside_client_and_config
    dataset_name = "test_dataset"

    # Create dataset on high side
    high_dataset_manager = SyftDatasetManager(syftbox_client=highside_client)

    # Create dataset with mock and private data
    dataset = high_dataset_manager.create(
        name=dataset_name,
        mock_path=DATA_DIR / "mock_data.csv",
        private_path=DATA_DIR / "private_data.csv",
        readme_path=DATA_DIR / "README.md",
    )

    # Verify dataset exists on high side
    assert dataset.name == dataset_name
    assert dataset.mock_dir.exists()
    assert dataset.private_dir.exists()
    assert (dataset.mock_dir / "mock_data.csv").exists()
    assert (dataset.private_dir / "private_data.csv").exists()

    # Sync dataset to low side
    syhl.sync_dataset(
        dataset_name=dataset_name,
        highside_client=highside_client,
        lowside_client=lowside_client,
        verbose=True,
    )

    # Verify dataset exists on low side with mock data only
    low_dataset_manager = SyftDatasetManager(syftbox_client=lowside_client)
    low_datasets = low_dataset_manager.get_all()

    assert len(low_datasets) == 1
    low_dataset = low_datasets[0]
    assert low_dataset.name == dataset_name

    # Verify mock data exists on low side
    assert low_dataset.mock_dir.exists()
    assert (low_dataset.mock_dir / "mock_data.csv").exists()

    # Verify private data does NOT exist on low side
    with pytest.raises(FileNotFoundError):
        _ = low_dataset.private_dir

    # Verify README exists on low side
    assert low_dataset.readme_path.exists()


def test_dataset_name_added_to_config(highside_client_and_config, lowside_client):
    """Test that dataset names are properly added to runtime config files."""
    highside_client, sync_config, _ = highside_client_and_config
    dataset_name = "test_dataset"

    # Create and sync dataset
    high_dataset_manager = SyftDatasetManager(syftbox_client=highside_client)
    high_dataset_manager.create(
        name=dataset_name,
        mock_path=DATA_DIR / "mock_data.csv",
        private_path=DATA_DIR / "private_data.csv",
        readme_path=DATA_DIR / "README.md",
    )

    syhl.sync_dataset(
        dataset_name=dataset_name,
        highside_client=highside_client,
        lowside_client=lowside_client,
    )

    # Verify dataset name is added to high-side config
    high_runtime_dir = sync_config.high_low_runtime_dir(Side.HIGH)
    high_config = HighLowRuntimeConfig.from_yaml(high_runtime_dir / "config.yaml")
    assert dataset_name in high_config.datasets

    # Verify dataset name is added to low-side config
    low_runtime_dir = sync_config.high_low_runtime_dir(Side.LOW)
    low_config = HighLowRuntimeConfig.from_yaml(low_runtime_dir / "config.yaml")
    assert dataset_name in low_config.datasets


def test_multiple_datasets_sync(highside_client_and_config, lowside_client):
    """Test syncing multiple datasets and verify config tracking."""
    highside_client, sync_config, _ = highside_client_and_config
    dataset_names = ["dataset_1", "dataset_2", "dataset_3"]

    # Create multiple datasets on high side
    high_dataset_manager = SyftDatasetManager(syftbox_client=highside_client)

    for dataset_name in dataset_names:
        high_dataset_manager.create(
            name=dataset_name,
            mock_path=DATA_DIR / "mock_data.csv",
            private_path=DATA_DIR / "private_data.csv",
            readme_path=DATA_DIR / "README.md",
        )

        # Sync each dataset
        syhl.sync_dataset(
            dataset_name=dataset_name,
            highside_client=highside_client,
            lowside_client=lowside_client,
        )

    # Verify all datasets exist on low side
    low_dataset_manager = SyftDatasetManager(syftbox_client=lowside_client)
    low_datasets = low_dataset_manager.get_all()

    assert len(low_datasets) == len(dataset_names)
    low_dataset_names = [d.name for d in low_datasets]
    for dataset_name in dataset_names:
        assert dataset_name in low_dataset_names

    # Verify all dataset names are in config files
    high_runtime_dir = sync_config.high_low_runtime_dir(Side.HIGH)
    high_config = HighLowRuntimeConfig.from_yaml(high_runtime_dir / "config.yaml")

    low_runtime_dir = sync_config.high_low_runtime_dir(Side.LOW)
    low_config = HighLowRuntimeConfig.from_yaml(low_runtime_dir / "config.yaml")

    for dataset_name in dataset_names:
        assert dataset_name in high_config.datasets
        assert dataset_name in low_config.datasets


def test_sync_config_persistence(highside_client_and_config):
    """Test that sync configuration can be saved and loaded correctly."""
    highside_client, sync_config, highside_identifier = highside_client_and_config

    # Save sync config
    sync_config.save(highside_client)

    # Load sync config
    loaded_config = RsyncConfig.load(highside_client)

    # Verify loaded config matches original
    assert loaded_config.high_side_name == sync_config.high_side_name
    assert loaded_config.syftbox_client_email == sync_config.syftbox_client_email
    assert len(loaded_config.entries) == len(sync_config.entries)
    assert loaded_config.high_syftbox_dir == sync_config.high_syftbox_dir
    assert loaded_config.low_syftbox_dir == sync_config.low_syftbox_dir


def test_error_handling_nonexistent_dataset(highside_client_and_config, lowside_client):
    """Test error handling when trying to sync non-existent dataset."""
    highside_client, _, _ = highside_client_and_config

    with pytest.raises(
        FileNotFoundError, match="Dataset 'nonexistent_dataset' not found on high-side"
    ):
        syhl.sync_dataset(
            dataset_name="nonexistent_dataset",
            highside_client=highside_client,
            lowside_client=lowside_client,
        )
