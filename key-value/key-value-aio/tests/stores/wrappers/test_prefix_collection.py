import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_collections import PrefixCollectionsWrapper
from key_value.shared.constants import DEFAULT_COLLECTION_NAME
from tests.stores.base import BaseStoreTests


class TestPrefixCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixCollectionsWrapper:
        return PrefixCollectionsWrapper(key_value=memory_store, prefix="collection_prefix")

    async def test_collection_isolation(self, memory_store: MemoryStore) -> None:
        """Test that different prefix wrappers create isolated collection spaces."""
        wrapper1 = PrefixCollectionsWrapper(key_value=memory_store, prefix="p1")
        wrapper2 = PrefixCollectionsWrapper(key_value=memory_store, prefix="p2")

        # Put same key/collection in both wrappers
        await wrapper1.put(collection="col", key="key", value={"source": "wrapper1"})
        await wrapper2.put(collection="col", key="key", value={"source": "wrapper2"})

        # Each wrapper should see its own value
        result1 = await wrapper1.get(collection="col", key="key")
        result2 = await wrapper2.get(collection="col", key="key")
        assert result1 == {"source": "wrapper1"}
        assert result2 == {"source": "wrapper2"}

        # After deleting from wrapper1, wrapper2 should still have its value
        await wrapper1.delete(collection="col", key="key")
        assert await wrapper1.get(collection="col", key="key") is None
        assert await wrapper2.get(collection="col", key="key") == {"source": "wrapper2"}

    async def test_default_collection_prefixed(self, memory_store: MemoryStore) -> None:
        """Test that default collection is prefixed when None is provided."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="default_prefix")

        # Put and get with None collection
        await wrapper.put(collection=None, key="key", value={"v": 1})
        result = await wrapper.get(collection=None, key="key")
        assert result == {"v": 1}

        # Verify the data persists
        result2 = await wrapper.get(collection=None, key="key")
        assert result2 == {"v": 1}

    async def test_custom_default_collection(self, memory_store: MemoryStore) -> None:
        """Test that custom default collection is used when specified."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="p", default_collection="custom")

        # Put and get with None collection
        await wrapper.put(collection=None, key="key", value={"v": 1})
        result = await wrapper.get(collection=None, key="key")
        assert result == {"v": 1}

        # Verify the data persists
        result2 = await wrapper.get(collection=None, key="key")
        assert result2 == {"v": 1}

    async def test_collection_get_many(self, memory_store: MemoryStore) -> None:
        """Test that get_many applies prefix to collection."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="col")

        # Put multiple values through wrapper
        await wrapper.put_many(collection="data", keys=["k1", "k2", "k3"], values=[{"v": 1}, {"v": 2}, {"v": 3}])

        # Get many should work correctly
        results = await wrapper.get_many(collection="data", keys=["k1", "k2", "k3"])
        assert results == [{"v": 1}, {"v": 2}, {"v": 3}]

        # Get many with missing keys should still work
        results_with_missing = await wrapper.get_many(collection="data", keys=["k1", "missing", "k3"])
        assert results_with_missing[0] == {"v": 1}
        assert results_with_missing[1] is None
        assert results_with_missing[2] == {"v": 3}

    async def test_collection_put_many(self, memory_store: MemoryStore) -> None:
        """Test that put_many applies prefix to collection."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="batch")

        # Put many through wrapper
        await wrapper.put_many(collection="users", keys=["a", "b", "c"], values=[{"u": "a"}, {"u": "b"}, {"u": "c"}])

        # Verify we can retrieve all values
        result_a = await wrapper.get(collection="users", key="a")
        result_b = await wrapper.get(collection="users", key="b")
        result_c = await wrapper.get(collection="users", key="c")
        assert result_a == {"u": "a"}
        assert result_b == {"u": "b"}
        assert result_c == {"u": "c"}

    async def test_collection_delete_isolation(self, memory_store: MemoryStore) -> None:
        """Test that delete only affects prefixed collections."""
        wrapper1 = PrefixCollectionsWrapper(key_value=memory_store, prefix="w1")
        wrapper2 = PrefixCollectionsWrapper(key_value=memory_store, prefix="w2")

        # Put same collection/key through both wrappers
        await wrapper1.put(collection="col", key="key", value={"source": "wrapper1"})
        await wrapper2.put(collection="col", key="key", value={"source": "wrapper2"})

        # Delete through wrapper1 should only affect wrapper1's collection
        deleted = await wrapper1.delete(collection="col", key="key")
        assert deleted is True

        # wrapper1 should not have the key
        result1 = await wrapper1.get(collection="col", key="key")
        assert result1 is None

        # wrapper2 should still have its collection's key
        result2 = await wrapper2.get(collection="col", key="key")
        assert result2 == {"source": "wrapper2"}

    async def test_collection_delete_many(self, memory_store: MemoryStore) -> None:
        """Test that delete_many applies prefix to collection."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="del")

        # Put multiple values
        await wrapper.put_many(collection="items", keys=["k1", "k2", "k3"], values=[{"v": 1}, {"v": 2}, {"v": 3}])

        # Delete many
        deleted = await wrapper.delete_many(collection="items", keys=["k1", "k2"])
        assert deleted == 2

        # k1 and k2 should be gone, k3 should remain
        result_k1 = await wrapper.get(collection="items", key="k1")
        result_k2 = await wrapper.get(collection="items", key="k2")
        result_k3 = await wrapper.get(collection="items", key="k3")
        assert result_k1 is None
        assert result_k2 is None
        assert result_k3 == {"v": 3}

    async def test_collection_ttl(self, memory_store: MemoryStore) -> None:
        """Test that ttl operation applies prefix correctly."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="ttl")

        # Put value with TTL through wrapper
        await wrapper.put(collection="data", key="key", value={"data": "value"}, ttl=100)

        # Query ttl through wrapper
        result, ttl = await wrapper.ttl(collection="data", key="key")
        assert result == {"data": "value"}
        assert ttl is not None
        assert ttl <= 100

    async def test_collection_ttl_many(self, memory_store: MemoryStore) -> None:
        """Test that ttl_many applies prefix to collection."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="multi_ttl")

        # Put values with TTL
        await wrapper.put_many(collection="cache", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}], ttl=60)

        # Query ttl for multiple keys
        results = await wrapper.ttl_many(collection="cache", keys=["k1", "k2", "k3"])
        assert results[0][0] == {"v": 1}
        assert results[1][0] == {"v": 2}
        assert results[2][0] is None

    async def test_multiple_collections_with_same_keys(self, memory_store: MemoryStore) -> None:
        """Test that same keys can exist in different collections without conflict."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="multi")

        # Put same key in different collections
        await wrapper.put(collection="col1", key="key", value={"col": "col1"})
        await wrapper.put(collection="col2", key="key", value={"col": "col2"})

        # Each collection should have its own value
        result1 = await wrapper.get(collection="col1", key="key")
        result2 = await wrapper.get(collection="col2", key="key")
        assert result1 == {"col": "col1"}
        assert result2 == {"col": "col2"}

    async def test_collection_with_special_characters(self, memory_store: MemoryStore) -> None:
        """Test that collection names with special characters are handled correctly."""
        wrapper = PrefixCollectionsWrapper(key_value=memory_store, prefix="special")

        # Put with special character collection names
        await wrapper.put(collection="users:2024", key="id", value={"year": 2024})
        await wrapper.put(collection="data@host", key="id", value={"host": "example.com"})

        # Get should work correctly
        result1 = await wrapper.get(collection="users:2024", key="id")
        result2 = await wrapper.get(collection="data@host", key="id")
        assert result1 == {"year": 2024}
        assert result2 == {"host": "example.com"}
