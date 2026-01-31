import pytest
from typing_extensions import override

from key_value.aio.errors import ReadOnlyError
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.read_only import ReadOnlyWrapper


class TestReadOnlyWrapper:
    @pytest.fixture
    async def memory_store(self) -> MemoryStore:
        return MemoryStore()

    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> ReadOnlyWrapper:
        # Pre-populate the store with test data
        await memory_store.put(collection="test", key="test", value={"test": "test"})
        return ReadOnlyWrapper(key_value=memory_store, raise_on_write=False)

    async def test_read_operations_allowed(self, memory_store: MemoryStore):
        # Pre-populate store
        await memory_store.put(collection="test", key="test", value={"test": "value"})

        read_only_store = ReadOnlyWrapper(key_value=memory_store, raise_on_write=True)

        # Read operations should work
        result = await read_only_store.get(collection="test", key="test")
        assert result == {"test": "value"}

        results = await read_only_store.get_many(collection="test", keys=["test"])
        assert results == [{"test": "value"}]

        value, _ = await read_only_store.ttl(collection="test", key="test")
        assert value == {"test": "value"}

    async def test_write_operations_raise_error(self, memory_store: MemoryStore):
        read_only_store = ReadOnlyWrapper(key_value=memory_store, raise_on_write=True)

        # Write operations should raise ReadOnlyError
        with pytest.raises(ReadOnlyError):
            await read_only_store.put(collection="test", key="test", value={"test": "value"})

        with pytest.raises(ReadOnlyError):
            await read_only_store.put_many(collection="test", keys=["test"], values=[{"test": "value"}])

        with pytest.raises(ReadOnlyError):
            await read_only_store.delete(collection="test", key="test")

        with pytest.raises(ReadOnlyError):
            await read_only_store.delete_many(collection="test", keys=["test"])

    async def test_write_operations_silent_ignore(self, memory_store: MemoryStore):
        read_only_store = ReadOnlyWrapper(key_value=memory_store, raise_on_write=False)

        # Write operations should be silently ignored
        await read_only_store.put(collection="test", key="new_key", value={"test": "value"})

        # Verify nothing was written
        result = await memory_store.get(collection="test", key="new_key")
        assert result is None

        # Delete should return False
        deleted = await read_only_store.delete(collection="test", key="test")
        assert deleted is False

        # Delete many should return 0
        deleted_count = await read_only_store.delete_many(collection="test", keys=["test"])
        assert deleted_count == 0
