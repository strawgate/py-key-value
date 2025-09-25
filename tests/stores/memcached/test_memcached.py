import asyncio
import contextlib
from collections.abc import AsyncGenerator

import pytest
from pymemcache.client.base import Client
from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.memcached import MemcachedStore
from tests.stores.conftest import BaseStoreTests

# Memcached test configuration
MEMCACHED_HOST = "localhost"
MEMCACHED_PORT = 11211

WAIT_FOR_MEMCACHED_TIMEOUT = 30


async def ping_memcached() -> bool:
    client = Client((MEMCACHED_HOST, MEMCACHED_PORT))
    try:
        client.set("__test__", "test_value", expire=1)
        result = client.get("__test__")
        client.delete("__test__")
    except Exception:
        return False
    else:
        return result is not None


async def wait_memcached() -> bool:
    # with a timeout of 30 seconds
    for _ in range(WAIT_FOR_MEMCACHED_TIMEOUT):
        if await ping_memcached():
            return True
        await asyncio.sleep(1)
    return False


@pytest.mark.skip_on_ci
class TestMemcachedStore(BaseStoreTests):
    @pytest.fixture
    @override
    async def store(self) -> AsyncGenerator[BaseKVStore, None]:
        if not await wait_memcached():
            pytest.skip("Memcached is not available")

        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        yield store

        # Cleanup - flush all keys
        with contextlib.suppress(Exception):
            store._client.flush_all()

    async def test_memcached_store_initialization(self):
        """Test that MemcachedStore can be initialized with different parameters."""
        # Test with host and port
        store1 = MemcachedStore(host="localhost", port=11211)
        assert store1._client is not None

        # Test with existing client
        client = Client(("localhost", 11211))
        store2 = MemcachedStore(client=client)
        assert store2._client is client

    async def test_memcached_store_long_keys(self):
        """Test that memcached store handles long keys correctly by hashing them."""
        if not await wait_memcached():
            pytest.skip("Memcached is not available")

        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        await store.setup()

        # Create a very long key that exceeds memcached's 250 character limit
        long_collection = "a" * 200
        long_key = "b" * 200
        test_value = {"test": "value"}

        # This should work despite the long key
        await store.put(collection=long_collection, key=long_key, value=test_value)
        result = await store.get(collection=long_collection, key=long_key)

        assert result == test_value

        # Cleanup
        await store.delete(collection=long_collection, key=long_key)

    async def test_memcached_store_limitations(self):
        """Test that memcached store correctly handles its limitations."""
        if not await wait_memcached():
            pytest.skip("Memcached is not available")

        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        await store.setup()

        # Set some test data
        await store.put(collection="test", key="key1", value={"test": "value1"})
        await store.put(collection="test", key="key2", value={"test": "value2"})

        # Test that keys() returns empty list (memcached limitation)
        keys = await store.keys(collection="test")
        assert keys == []

        # Test that clear_collection() returns 0 (memcached limitation)
        cleared = await store.clear_collection(collection="test")
        assert cleared == 0

        # Test that list_collections() returns empty list (memcached limitation)
        collections = await store.list_collections()
        assert collections == []

        # Cleanup
        store._client.flush_all()

    async def test_memcached_store_ttl(self):
        """Test TTL functionality with memcached."""
        if not await wait_memcached():
            pytest.skip("Memcached is not available")

        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        await store.setup()

        # Test with TTL
        await store.put(collection="test", key="ttl_key", value={"test": "value"}, ttl=2)

        # Should exist immediately
        result = await store.get(collection="test", key="ttl_key")
        assert result == {"test": "value"}

        # Wait for expiration
        await asyncio.sleep(3)

        # Should be expired now
        result = await store.get(collection="test", key="ttl_key")
        assert result is None

    async def test_memcached_store_connection_error(self):
        """Test that connection errors are properly handled."""
        # Create store with invalid port to test connection error
        store = MemcachedStore(host="localhost", port=99999)

        with pytest.raises(StoreConnectionError):
            await store.setup()
