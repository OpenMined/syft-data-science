from syft_rds.store import YAMLFileSystemDatabase
from tests.mocks import MockUserSpec

def test_create_record(mock_user_store: YAMLFileSystemDatabase, mock_user_1: MockUserSpec):
    record_id = mock_user_store.create(mock_user_1)

    assert record_id == mock_user_1.id
    assert mock_user_store.read(record_id) == mock_user_1
    assert mock_user_store.list_all() == [mock_user_1]

def test_update_record(mock_user_store: YAMLFileSystemDatabase, mock_user_1: MockUserSpec):
    record_id = mock_user_store.create(mock_user_1)
    mock_user_1.name = "Alice Smith"
    updated_record: MockUserSpec = mock_user_store.update(record_id, mock_user_1)

    assert updated_record is not None
    assert updated_record.name == "Alice Smith"

def test_delete_record(mock_user_store: YAMLFileSystemDatabase, mock_user_1: MockUserSpec):
    record_id = mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Delete the Record
    res = mock_user_store.delete(record_id)
    assert res
    assert len(mock_user_store.list_all()) == 0

def test_query_record(mock_user_store: YAMLFileSystemDatabase, mock_user_1: MockUserSpec):
    mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Query the Record
    query = {"name": "Alice"}
    results = mock_user_store.query(**query)
    assert len(results) == 1
    assert results[0] == mock_user_1

def test_search_record(mock_user_store: YAMLFileSystemDatabase, mock_user_1: MockUserSpec):
    mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Search the Record
    results = mock_user_store.search(query=mock_user_1.email, fields=["email"])
    assert len(results) == 1
    assert results[0] == mock_user_1



    
