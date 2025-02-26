from syft_rds.store import BaseSpec , YAMLFileSystemDatabase

class MockUserSpec(BaseSpec):
    __spec_name__ = "user"

    name: str
    email: str

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



    
