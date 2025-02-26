from syft_rds.store import BaseSpec , YAMLFileSystemDatabase

class MockUserSpec(BaseSpec):
    __spec_name__ = "user"

    name: str
    email: str
    tags: list[str] = []

def test_create_record(yaml_store):
    user_store: YAMLFileSystemDatabase = yaml_store(MockUserSpec)
    user = MockUserSpec(name="Alice", email="alice@openmined.org")

    record_id = user_store.create(user)
    assert record_id == user.id
    assert user_store.read(record_id) == user
    assert user_store.list_all() == [user]

def test_update_record(yaml_store):
    user_store: YAMLFileSystemDatabase = yaml_store(MockUserSpec)
    user= MockUserSpec(name="Alice", email="alice@openmined.org")

    record_id = user_store.create(user)
    user.name = "Alice Smith"
    updated_record: MockUserSpec = user_store.update(record_id, user)
    assert updated_record is not None
    assert updated_record.name == "Alice Smith"

def test_delete_record(yaml_store):
    user_store: YAMLFileSystemDatabase = yaml_store(MockUserSpec)
    user = MockUserSpec(name="Alice", email="alice@openmined.org")

    record_id = user_store.create(user)
    assert len(user_store.list_all()) == 1

    # Delete the Record
    res = user_store.delete(record_id)
    assert res
    assert len(user_store.list_all()) == 0

def test_query_record(yaml_store):
    user_store: YAMLFileSystemDatabase = yaml_store(MockUserSpec)
    user = MockUserSpec(name="Alice", email="alice@openmined.org")

    user_store.create(user)
    assert len(user_store.list_all()) == 1

    # Query the Record
    query = {"name": "Alice"}
    results = user_store.query(**query)
    assert len(results) == 1
    assert results[0] == user

def test_search_record(yaml_store):
    user_store: YAMLFileSystemDatabase = yaml_store(MockUserSpec)
    user = MockUserSpec(name="Alice", email="alice@openmined.org", tags=["tag1", "tag2"])

    user_store.create(user)
    assert len(user_store.list_all()) == 1

    # Search the Record
    results = user_store.search(query="tag1", fields=["tags"])
    assert len(results) == 1
    assert results[0] == user





    
