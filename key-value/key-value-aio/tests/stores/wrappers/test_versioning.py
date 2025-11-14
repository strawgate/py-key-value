import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.versioning import VersioningWrapper
from tests.stores.base import BaseStoreTests


class TestVersioningWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> VersioningWrapper:
        return VersioningWrapper(key_value=memory_store, version="v1")

    async def test_versioning_wraps_and_unwraps_value(self, memory_store: MemoryStore):
        versioned_store = VersioningWrapper(key_value=memory_store, version="v1")

        # Put a value
        await versioned_store.put(collection="test", key="test", value={"data": "value"})

        # Get it back
        result = await versioned_store.get(collection="test", key="test")
        assert result == {"data": "value"}

    async def test_versioning_stores_version_metadata(self, memory_store: MemoryStore):
        versioned_store = VersioningWrapper(key_value=memory_store, version="v1")

        # Put a value through versioned wrapper
        await versioned_store.put(collection="test", key="test", value={"data": "value"})

        # Check raw value in underlying store
        raw_value = await memory_store.get(collection="test", key="test")
        assert raw_value is not None
        assert "__version__" in raw_value
        assert raw_value["__version__"] == "v1"
        assert "__versioned_data__" in raw_value
        assert raw_value["__versioned_data__"] == {"data": "value"}

    async def test_versioning_returns_none_for_version_mismatch(self, memory_store: MemoryStore):
        store_v1 = VersioningWrapper(key_value=memory_store, version="v1")
        store_v2 = VersioningWrapper(key_value=memory_store, version="v2")

        # Store with v1
        await store_v1.put(collection="test", key="test", value={"data": "value"})

        # Try to retrieve with v2
        result = await store_v2.get(collection="test", key="test")
        assert result is None  # Version mismatch should return None

    async def test_versioning_handles_unversioned_data(self, memory_store: MemoryStore):
        versioned_store = VersioningWrapper(key_value=memory_store, version="v1")

        # Put unversioned data directly in underlying store
        await memory_store.put(collection="test", key="test", value={"data": "value"})

        # Should return the data as-is (backward compatibility)
        result = await versioned_store.get(collection="test", key="test")
        assert result == {"data": "value"}

    async def test_versioning_with_integer_version(self, memory_store: MemoryStore):
        store_v1 = VersioningWrapper(key_value=memory_store, version=1)
        store_v2 = VersioningWrapper(key_value=memory_store, version=2)

        # Store with version 1
        await store_v1.put(collection="test", key="test", value={"data": "value"})

        # Retrieve with version 1
        result = await store_v1.get(collection="test", key="test")
        assert result == {"data": "value"}

        # Should fail with version 2
        result = await store_v2.get(collection="test", key="test")
        assert result is None

    async def test_versioning_get_many(self, memory_store: MemoryStore):
        store_v1 = VersioningWrapper(key_value=memory_store, version="v1")
        store_v2 = VersioningWrapper(key_value=memory_store, version="v2")

        # Store some values with v1
        await store_v1.put(collection="test", key="key1", value={"data": "value1"})
        await store_v1.put(collection="test", key="key2", value={"data": "value2"})

        # Store some values with v2
        await store_v2.put(collection="test", key="key3", value={"data": "value3"})

        # Get all keys with v1 wrapper
        results = await store_v1.get_many(collection="test", keys=["key1", "key2", "key3"])

        # Should get v1 values, but None for v2 value
        assert results[0] == {"data": "value1"}
        assert results[1] == {"data": "value2"}
        assert results[2] is None  # Version mismatch

    async def test_versioning_ttl(self, memory_store: MemoryStore):
        store_v1 = VersioningWrapper(key_value=memory_store, version="v1")
        store_v2 = VersioningWrapper(key_value=memory_store, version="v2")

        # Store with TTL
        await store_v1.put(collection="test", key="test", value={"data": "value"}, ttl=60.0)

        # Get with matching version
        value, ttl = await store_v1.ttl(collection="test", key="test")
        assert value == {"data": "value"}
        assert ttl is not None
        assert ttl > 0

        # Get with mismatched version
        value, ttl = await store_v2.ttl(collection="test", key="test")
        assert value is None
        assert ttl is None  # TTL should also be None for version mismatch

    async def test_versioning_ttl_many(self, memory_store: MemoryStore):
        store_v1 = VersioningWrapper(key_value=memory_store, version="v1")
        store_v2 = VersioningWrapper(key_value=memory_store, version="v2")

        # Store values with different versions
        await store_v1.put(collection="test", key="key1", value={"data": "value1"}, ttl=60.0)
        await store_v2.put(collection="test", key="key2", value={"data": "value2"}, ttl=60.0)

        # Get with v1 wrapper
        results = await store_v1.ttl_many(collection="test", keys=["key1", "key2"])

        # First should have value and TTL, second should be None/None
        assert results[0][0] == {"data": "value1"}
        assert results[0][1] is not None
        assert results[1][0] is None
        assert results[1][1] is None

    async def test_versioning_put_many(self, memory_store: MemoryStore):
        versioned_store = VersioningWrapper(key_value=memory_store, version="v1")

        # Put multiple values
        await versioned_store.put_many(
            collection="test", keys=["key1", "key2", "key3"], values=[{"data": "value1"}, {"data": "value2"}, {"data": "value3"}]
        )

        # Verify all are versioned
        for i in range(1, 4):
            raw_value = await memory_store.get(collection="test", key=f"key{i}")
            assert raw_value is not None
            assert raw_value["__version__"] == "v1"
            assert raw_value["__versioned_data__"] == {"data": f"value{i}"}

    async def test_versioning_doesnt_double_wrap(self, memory_store: MemoryStore):
        versioned_store = VersioningWrapper(key_value=memory_store, version="v1")

        # Put a value that already has version metadata
        await versioned_store.put(collection="test", key="test", value={"__version__": "v1", "__versioned_data__": {"data": "value"}})

        # Check it wasn't double-wrapped
        raw_value = await memory_store.get(collection="test", key="test")
        assert raw_value is not None
        assert raw_value == {"__version__": "v1", "__versioned_data__": {"data": "value"}}
        # Should not have nested version keys
        assert "__versioned_data__" in raw_value
        assert "__version__" not in raw_value.get("__versioned_data__", {})

    async def test_versioning_schema_evolution_scenario(self, memory_store: MemoryStore):
        """Test a realistic schema evolution scenario."""
        # Application v1: Store user with name and email
        app_v1 = VersioningWrapper(key_value=memory_store, version="user_schema_v1")
        await app_v1.put(collection="users", key="user:123", value={"name": "John Doe", "email": "john@example.com"})

        # Application v2: Expects users to have name, email, and age
        app_v2 = VersioningWrapper(key_value=memory_store, version="user_schema_v2")

        # When v2 tries to read old data, it gets None (cache miss)
        result = await app_v2.get(collection="users", key="user:123")
        assert result is None

        # Application can then reload from authoritative source with new schema
        await app_v2.put(collection="users", key="user:123", value={"name": "John Doe", "email": "john@example.com", "age": 30})

        # Now v2 can read it
        result = await app_v2.get(collection="users", key="user:123")
        assert result == {"name": "John Doe", "email": "john@example.com", "age": 30}

        # But v1 still gets None (cache invalidation works both ways)
        result = await app_v1.get(collection="users", key="user:123")
        assert result is None
