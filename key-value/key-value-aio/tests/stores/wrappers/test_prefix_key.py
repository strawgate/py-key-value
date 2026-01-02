import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.prefix_keys import PrefixKeysWrapper
from tests.stores.base import BaseStoreTests


class TestPrefixKeyWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> PrefixKeysWrapper:
        return PrefixKeysWrapper(key_value=memory_store, prefix="key_prefix")

    async def test_prefix_key_isolation(self, memory_store: MemoryStore):
        """Test that prefixed keys are isolated from unprefixed keys."""
        store = PrefixKeysWrapper(key_value=memory_store, prefix="prefix1")

        # Store a value with the prefix wrapper
        await store.put(collection="test", key="test_key", value={"data": "prefixed"})

        # Check that the underlying store has the prefixed key
        underlying_value = await memory_store.get(collection="test", key="prefix1__test_key")
        assert underlying_value == {"data": "prefixed"}

        # Check that the unprefixed key doesn't exist
        unprefixed_value = await memory_store.get(collection="test", key="test_key")
        assert unprefixed_value is None

    async def test_prefix_key_multiple_prefixes(self, memory_store: MemoryStore):
        """Test that different prefixes maintain separate namespaces."""
        store1 = PrefixKeysWrapper(key_value=memory_store, prefix="p1")
        store2 = PrefixKeysWrapper(key_value=memory_store, prefix="p2")

        # Store same key in both prefixes
        await store1.put(collection="test", key="shared_key", value={"data": "store1"})
        await store2.put(collection="test", key="shared_key", value={"data": "store2"})

        # Retrieve and verify isolation
        value1 = await store1.get(collection="test", key="shared_key")
        value2 = await store2.get(collection="test", key="shared_key")
        assert value1 == {"data": "store1"}
        assert value2 == {"data": "store2"}
