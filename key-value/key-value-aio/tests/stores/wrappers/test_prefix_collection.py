import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_collections import PrefixCollectionsWrapper
from tests.stores.base import BaseStoreTests


class TestPrefixCollectionWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixCollectionsWrapper:
        return PrefixCollectionsWrapper(key_value=memory_store, prefix="collection_prefix")

    async def test_collection_prefix_isolation(self, memory_store: MemoryStore):
        """Test that prefixed collections are isolated from unprefixed collections."""
        store = PrefixCollectionsWrapper(key_value=memory_store, prefix="prefix1")

        # Store a value with the prefix wrapper
        await store.put(collection="test_collection", key="test_key", value={"data": "prefixed"})

        # Check that the underlying store has the prefixed collection
        underlying_value = await memory_store.get(collection="prefix1__test_collection", key="test_key")
        assert underlying_value == {"data": "prefixed"}

        # Check that the unprefixed collection doesn't have the value
        unprefixed_value = await memory_store.get(collection="test_collection", key="test_key")
        assert unprefixed_value is None

    async def test_collection_prefix_default_collection(self, memory_store: MemoryStore):
        """Test that default collection is used when not specified."""
        store = PrefixCollectionsWrapper(key_value=memory_store, prefix="prefix1")

        # Store a value without specifying collection
        await store.put(key="test_key", value={"data": "default"})

        # Check that it was stored with the default collection
        underlying_value = await memory_store.get(collection="prefix1__default_collection", key="test_key")
        assert underlying_value == {"data": "default"}
