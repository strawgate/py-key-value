import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.single_collection import SingleCollectionWrapper
from tests.stores.base import BaseStoreTests


class TestSingleCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> SingleCollectionWrapper:
        return SingleCollectionWrapper(key_value=memory_store, single_collection="test")

    async def test_single_collection_aggregates_multiple_collections(self, memory_store: MemoryStore):
        """Test that multiple collections are aggregated into a single backing collection."""
        store = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Store data in multiple logical collections
        await store.put(collection="coll1", key="key1", value={"data": "coll1_data"})
        await store.put(collection="coll2", key="key1", value={"data": "coll2_data"})

        # Verify they are stored with different prefixes in the backing collection
        value1 = await memory_store.get(collection="backing", key="coll1__key1")
        value2 = await memory_store.get(collection="backing", key="coll2__key1")

        assert value1 == {"data": "coll1_data"}
        assert value2 == {"data": "coll2_data"}

        # Verify retrieval through wrapper works correctly
        retrieved1 = await store.get(collection="coll1", key="key1")
        retrieved2 = await store.get(collection="coll2", key="key1")

        assert retrieved1 == {"data": "coll1_data"}
        assert retrieved2 == {"data": "coll2_data"}

    async def test_single_collection_default_collection(self, memory_store: MemoryStore):
        """Test that default collection is used correctly."""
        store = SingleCollectionWrapper(key_value=memory_store, single_collection="backing")

        # Store without specifying collection
        await store.put(key="key1", value={"data": "default_collection"})

        # Verify it was stored with the default collection prefix
        underlying_value = await memory_store.get(collection="backing", key="default_collection__key1")
        assert underlying_value == {"data": "default_collection"}
