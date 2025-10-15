import pytest
from key_value.shared.errors import EntryTooLargeError
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.limit_size import LimitSizeWrapper
from tests.stores.base import BaseStoreTests


class TestLimitSizeWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> LimitSizeWrapper:
        # Set a reasonable max size for normal test operations (20KB to handle large test strings)
        return LimitSizeWrapper(key_value=memory_store, max_size=20 * 1024, raise_on_error=False)

    async def test_put_within_limit(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=1024, raise_on_error=True)

        # Small value should succeed
        await limit_size_store.put(collection="test", key="test", value={"test": "test"})
        result = await limit_size_store.get(collection="test", key="test")
        assert result is not None
        assert result["test"] == "test"

    async def test_put_exceeds_limit_with_raise(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=100, raise_on_error=True)

        # Large value should raise an error
        large_value = {"data": "x" * 1000}
        with pytest.raises(EntryTooLargeError) as exc_info:
            await limit_size_store.put(collection="test", key="test", value=large_value)

        assert exc_info.value.extra_info["max_size"] == 100
        assert exc_info.value.extra_info["size"] > 100
        assert exc_info.value.extra_info["collection"] == "test"
        assert exc_info.value.extra_info["key"] == "test"

        # Verify nothing was stored
        result = await limit_size_store.get(collection="test", key="test")
        assert result is None

    async def test_put_exceeds_limit_without_raise(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=100, raise_on_error=False)

        # Large value should be silently ignored
        large_value = {"data": "x" * 1000}
        await limit_size_store.put(collection="test", key="test", value=large_value)

        # Verify nothing was stored
        result = await limit_size_store.get(collection="test", key="test")
        assert result is None

    async def test_put_many_mixed_sizes_with_raise(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=100, raise_on_error=True)

        # Mix of small and large values
        keys = ["small1", "large1", "small2"]
        values = [{"data": "x"}, {"data": "x" * 1000}, {"data": "y"}]

        # Should raise on the large value
        with pytest.raises(EntryTooLargeError):
            await limit_size_store.put_many(collection="test", keys=keys, values=values)

        # Verify nothing was stored due to the error
        results = await limit_size_store.get_many(collection="test", keys=keys)
        assert results[0] is None
        assert results[1] is None
        assert results[2] is None

    async def test_put_many_mixed_sizes_without_raise(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=100, raise_on_error=False)

        # Mix of small and large values
        keys = ["small1", "large1", "small2"]
        values = [{"data": "x"}, {"data": "x" * 1000}, {"data": "y"}]

        # Should silently filter out large value
        await limit_size_store.put_many(collection="test", keys=keys, values=values)

        # Verify only small values were stored
        results = await limit_size_store.get_many(collection="test", keys=keys)
        assert results[0] == {"data": "x"}
        assert results[1] is None  # Large value was filtered out
        assert results[2] == {"data": "y"}

    async def test_put_many_with_ttl_sequence(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=100, raise_on_error=False)

        # Mix of small and large values with TTLs
        keys = ["small1", "large1", "small2"]
        values = [{"data": "x"}, {"data": "x" * 1000}, {"data": "y"}]
        ttls = [100, 200, 300]

        # Should filter out large value and its corresponding TTL
        await limit_size_store.put_many(collection="test", keys=keys, values=values, ttl=ttls)

        # Verify only small values were stored
        results = await limit_size_store.get_many(collection="test", keys=keys)
        assert results[0] == {"data": "x"}
        assert results[1] is None  # Large value was filtered out
        assert results[2] == {"data": "y"}

    async def test_put_many_all_too_large_without_raise(self, memory_store: MemoryStore):
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(key_value=memory_store, max_size=10, raise_on_error=False)

        # All values too large
        keys = ["key1", "key2"]
        values = [{"data": "x" * 1000}, {"data": "y" * 1000}]

        # Should not raise, but nothing should be stored
        await limit_size_store.put_many(collection="test", keys=keys, values=values)

        # Verify nothing was stored
        results = await limit_size_store.get_many(collection="test", keys=keys)
        assert results[0] is None
        assert results[1] is None

    async def test_exact_size_limit(self, memory_store: MemoryStore):
        # First, determine the exact size of a small value
        from key_value.shared.utils.managed_entry import ManagedEntry

        test_value = {"test": "value"}
        managed_entry = ManagedEntry(value=test_value)
        json_str = managed_entry.to_json()
        exact_size = len(json_str.encode("utf-8"))

        # Create a store with exact size limit
        limit_size_store: LimitSizeWrapper = LimitSizeWrapper(
            key_value=memory_store, max_size=exact_size, raise_on_error=True
        )

        # Should succeed at exact limit
        await limit_size_store.put(collection="test", key="test", value=test_value)
        result = await limit_size_store.get(collection="test", key="test")
        assert result == test_value

        # Should fail if one byte over
        limit_size_store_under: LimitSizeWrapper = LimitSizeWrapper(
            key_value=memory_store, max_size=exact_size - 1, raise_on_error=True
        )
        with pytest.raises(EntryTooLargeError):
            await limit_size_store_under.put(collection="test", key="test2", value=test_value)
