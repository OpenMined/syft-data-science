from syft_rds.store import YAMLStore
from tests.mocks import MockUserSchema


def test_create_record(mock_user_store: YAMLStore, mock_user_1: MockUserSchema):
    record = mock_user_store.create(mock_user_1)

    assert record.uid == mock_user_1.uid
    assert mock_user_store.get_by_uid(record.uid) == mock_user_1
    assert mock_user_store.list_all() == [mock_user_1]


def test_update_record(mock_user_store: YAMLStore, mock_user_1: MockUserSchema):
    record = mock_user_store.create(mock_user_1)
    mock_user_1.name = "Alice Smith"
    updated_record: MockUserSchema = mock_user_store.update(record.uid, mock_user_1)

    assert updated_record is not None
    assert updated_record.name == mock_user_1.name
    assert mock_user_store.get_by_uid(updated_record.uid) == mock_user_1


def test_delete_record(mock_user_store: YAMLStore, mock_user_1: MockUserSchema):
    record = mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Delete the Record
    res = mock_user_store.delete(record.uid)
    assert res
    assert len(mock_user_store.list_all()) == 0


def test_query_record(mock_user_store: YAMLStore, mock_user_1: MockUserSchema):
    mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Query the Record
    filters = {"name": "Alice"}
    results = mock_user_store.get_all(
        limit=100,
        offset=0,
        order_by="name",
        sort_order="asc",
        filters=filters,
    )
    assert len(results) == 1
    assert results[0] == mock_user_1


def test_search_record(mock_user_store: YAMLStore, mock_user_1: MockUserSchema):
    mock_user_store.create(mock_user_1)
    assert len(mock_user_store.list_all()) == 1

    # Search the Record
    results = mock_user_store.text_search(query=mock_user_1.email, fields=["email"])
    assert len(results) == 1
    assert results[0] == mock_user_1
