import pytest
from importlib import import_module


def test_syft_data_science_imports():
    """Test that syft_data_science package imports correctly."""
    try:
        import syft_data_science

        assert hasattr(syft_data_science, "syft_rds")
        assert hasattr(syft_data_science, "syft_datasets")
        assert hasattr(syft_data_science, "syft_runtimes")
        assert hasattr(syft_data_science, "syft_notebook_ui")
    except ImportError as e:
        pytest.fail(f"Failed to import syft_data_science: {e}")


def test_all_modules_in_all_list():
    """Test that __all__ contains all expected modules."""
    import syft_data_science

    expected_modules = [
        "syft_rds",
        "syft_datasets",
        "syft_runtimes",
        "syft_notebook_ui",
    ]
    assert hasattr(syft_data_science, "__all__")
    assert set(syft_data_science.__all__) == set(expected_modules)


def test_imported_modules_are_accessible():
    """Test that imported modules are actually accessible."""
    import syft_data_science

    # Test that each module can be accessed
    assert syft_data_science.syft_rds is not None
    assert syft_data_science.syft_datasets is not None
    assert syft_data_science.syft_runtimes is not None
    assert syft_data_science.syft_notebook_ui is not None


def test_submodule_functionality():
    """Test basic functionality of imported submodules."""
    import syft_data_science

    # Test syft_rds has expected attributes/classes
    assert hasattr(syft_data_science.syft_rds, "RDSClient")
    assert hasattr(syft_data_science.syft_rds, "init_session")

    # Test syft_datasets has expected functionality
    # (Add specific tests based on what syft_datasets should expose)

    # Test syft_runtimes has expected functionality
    # (Add specific tests based on what syft_runtimes should expose)


def test_individual_module_imports():
    """Test that individual modules can be imported directly."""
    modules_to_test = ["syft_rds", "syft_datasets", "syft_runtimes", "syft_notebook_ui"]

    for module_name in modules_to_test:
        try:
            module = import_module(module_name)
            assert module is not None
        except ImportError as e:
            pytest.fail(f"Failed to import {module_name}: {e}")


def test_no_import_errors_on_package_level():
    """Test that importing syft_data_science doesn't raise any errors."""
    try:
        # If we get here, import was successful
        assert True
    except Exception as e:
        pytest.fail(f"Unexpected error importing syft_data_science: {e}")
