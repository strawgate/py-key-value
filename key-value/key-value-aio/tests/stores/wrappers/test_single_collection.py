import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.single_collection import SingleCollectionWrapper
from key_value.shared.constants import DEFAULT_COLLECTION_NAME
from tests.stores.base import BaseStoreTests


class TestSingleCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> SingleCollectionWrapper:
        return SingleCollectionWrapper(key_value=memory_store, single_collection="test")

    async def test_single_collection_isolation(self, memory_store: MemoryStore) -> None:
        """Test that multiple logical collections are isolated within the single backing collection."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put same key in different logical collections
        await wrapper.put(collection="col1", key="key", value={"col": "col1"})
        await wrapper.put(collection="col2", key="key", value={"col": "col2"})

        # Each logical collection should have its own value
        result1 = await wrapper.get(collection="col1", key="key")
        result2 = await wrapper.get(collection="col2", key="key")
        assert result1 == {"col": "col1"}
        assert result2 == {"col": "col2"}

        # After deleting from col1, col2 should still have its value
        await wrapper.delete(collection="col1", key="key")
        assert await wrapper.get(collection="col1", key="key") is None
        assert await wrapper.get(collection="col2", key="key") == {"col": "col2"}

    async def test_default_collection_used(self, memory_store: MemoryStore) -> None:
        """Test that default collection is used when None is provided."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put and get with None collection
        await wrapper.put(collection=None, key="key", value={"v": 1})
        result = await wrapper.get(collection=None, key="key")
        assert result == {"v": 1}

        # Verify the data persists
        result2 = await wrapper.get(collection=None, key="key")
        assert result2 == {"v": 1}

    async def test_custom_default_collection(self, memory_store: MemoryStore) -> None:
        """Test that custom default collection is used when specified."""
        wrapper = SingleCollectionWrapper(
            key_value=memory_store, single_collection="backing", default_collection="custom"
        )

        # Put and get with None collection
        await wrapper.put(collection=None, key="key", value={"v": 1})
        result = await wrapper.get(collection=None, key="key")
        assert result == {"v": 1}

        # Verify the data persists
        result2 = await wrapper.get(collection=None, key="key")
        assert result2 == {"v": 1}

    async def test_single_collection_get_many(self, memory_store: MemoryStore) -> None:
        """Test that get_many works across logical collections."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put values in different logical collections
        await wrapper.put(collection="col1", key="k1", value={"col": "col1", "k": "k1"})
        await wrapper.put(collection="col2", key="k2", value={"col": "col2", "k": "k2"})

        # Get many from col1
        results = await wrapper.get_many(collection="col1", keys=["k1", "k2", "k3"])
        assert results[0] == {"col": "col1", "k": "k1"}
        assert results[1] is None
        assert results[2] is None

    async def test_single_collection_put_many(self, memory_store: MemoryStore) -> None:
        """Test that put_many stores all keys under the single backing collection."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put many in a logical collection
        await wrapper.put_many(collection="items", keys=["a", "b", "c"], values=[{"v": "a"}, {"v": "b"}, {"v": "c"}])

        # Verify all keys can be retrieved
        result_a = await wrapper.get(collection="items", key="a")
        result_b = await wrapper.get(collection="items", key="b")
        result_c = await wrapper.get(collection="items", key="c")
        assert result_a == {"v": "a"}
        assert result_b == {"v": "b"}
        assert result_c == {"v": "c"}

    async def test_single_collection_delete_isolation(self, memory_store: MemoryStore) -> None:
        """Test that deleting from one logical collection doesn't affect others."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put same key in different logical collections
        await wrapper.put(collection="col1", key="key", value={"col": "col1"})
        await wrapper.put(collection="col2", key="key", value={"col": "col2"})

        # Delete from col1
        deleted = await wrapper.delete(collection="col1", key="key")
        assert deleted is True

        # col1 should not have the key
        result1 = await wrapper.get(collection="col1", key="key")
        assert result1 is None

        # col2 should still have its key
        result2 = await wrapper.get(collection="col2", key="key")
        assert result2 == {"col": "col2"}

    async def test_single_collection_delete_many(self, memory_store: MemoryStore) -> None:
        """Test that delete_many works correctly with logical collections."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put multiple values in a logical collection
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

    async def test_single_collection_ttl(self, memory_store: MemoryStore) -> None:
        """Test that ttl operation works with logical collections."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put value with TTL in a logical collection
        await wrapper.put(collection="data", key="key", value={"data": "value"}, ttl=100)

        # Query ttl through wrapper
        result, ttl = await wrapper.ttl(collection="data", key="key")
        assert result == {"data": "value"}
        assert ttl is not None
        assert ttl <= 100

    async def test_single_collection_ttl_many(self, memory_store: MemoryStore) -> None:
        """Test that ttl_many works correctly with logical collections."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put values with TTL in a logical collection
        await wrapper.put_many(collection="cache", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}], ttl=60)

        # Query ttl for multiple keys
        results = await wrapper.ttl_many(collection="cache", keys=["k1", "k2", "k3"])
        assert results[0][0] == {"v": 1}
        assert results[1][0] == {"v": 2}
        assert results[2][0] is None

    async def test_single_collection_multiple_wrappers(self, memory_store: MemoryStore) -> None:
        """Test that multiple wrappers with different single collections are independent."""
        wrapper1 = SingleCollectionWrapper(key_value=memory_store, single_collection="backing1")
        wrapper2 = SingleCollectionWrapper(key_value=memory_store, single_collection="backing2")

        # Put same key/collection in both wrappers
        await wrapper1.put(collection="col", key="key", value={"wrapper": "wrapper1"})
        await wrapper2.put(collection="col", key="key", value={"wrapper": "wrapper2"})

        # Each wrapper should see its own value
        result1 = await wrapper1.get(collection="col", key="key")
        result2 = await wrapper2.get(collection="col", key="key")
        assert result1 == {"wrapper": "wrapper1"}
        assert result2 == {"wrapper": "wrapper2"}

        # After deleting from wrapper1, wrapper2 should still have its value
        await wrapper1.delete(collection="col", key="key")
        assert await wrapper1.get(collection="col", key="key") is None
        assert await wrapper2.get(collection="col", key="key") == {"wrapper": "wrapper2"}

    async def test_single_collection_custom_separator(self, memory_store: MemoryStore) -> None:
        """Test that custom separator is used for prefixing."""
        wrapper = SingleCollectionWrapper(
            key_value=memory_store, single_collection="backing", separator="|"
        )

        # Put and get with custom separator
        await wrapper.put(collection="col", key="key", value={"v": 1})
        result = await wrapper.get(collection="col", key="key")
        assert result == {"v": 1}

        # Data should use custom separator
        raw_result = await memory_store.get(collection="backing", key="col|key")
        assert raw_result == {"v": 1}

    async def test_single_collection_with_special_characters(self, memory_store: MemoryStore) -> None:
        """Test that special characters in collection and key names are handled correctly."""
        wrapper = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Put with special character names
        await wrapper.put(collection="col:2024", key="user@host", value={"user": "john"})
        result = await wrapper.get(collection="col:2024", key="user@host")
        assert result == {"user": "john"}
