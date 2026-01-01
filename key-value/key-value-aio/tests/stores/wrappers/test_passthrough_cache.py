import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.disk.store import DiskStore
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.passthrough_cache import PassthroughCacheWrapper
from tests.stores.base import BaseStoreTests

DISK_STORE_SIZE_LIMIT = 100 * 1024  # 100KB


class TestPassthroughCacheWrapper(BaseStoreTests):
    @pytest.fixture(scope="session")
    async def primary_store(self) -> AsyncGenerator[DiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            async with DiskStore(directory=temp_dir, max_size=DISK_STORE_SIZE_LIMIT) as disk_store:
                yield disk_store

    @pytest.fixture
    async def cache_store(self, memory_store: MemoryStore) -> MemoryStore:
        return memory_store

    @override
    @pytest.fixture
    async def store(self, primary_store: DiskStore, cache_store: MemoryStore) -> PassthroughCacheWrapper:
        primary_store._cache.clear()  # pyright: ignore[reportPrivateUsage]
        return PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

    async def test_ttl_caches_from_primary(self):
        """Test that ttl retrieves from primary and caches the result."""
        primary_store = MemoryStore()
        cache_store = MemoryStore()
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put data in primary with TTL
        await primary_store.put(collection="test", key="test", value={"v": "1"}, ttl=100)

        # Call ttl - should get from primary and cache it
        value, ttl = await wrapper.ttl(collection="test", key="test")
        assert value == {"v": "1"}
        assert ttl is not None

        # Verify it's now in cache
        cached_value = await cache_store.get(collection="test", key="test")
        assert cached_value == {"v": "1"}

    async def test_ttl_returns_cached_value(self):
        """Test that ttl returns cached value when available."""
        primary_store = MemoryStore()
        cache_store = MemoryStore()
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put data only in cache
        await cache_store.put(collection="test", key="test", value={"v": "cached"}, ttl=100)

        # Call ttl - should return cached value
        value, ttl = await wrapper.ttl(collection="test", key="test")
        assert value == {"v": "cached"}
        assert ttl is not None

    async def test_ttl_returns_none_for_missing(self):
        """Test that ttl returns (None, None) for missing entries."""
        primary_store = MemoryStore()
        cache_store = MemoryStore()
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Call ttl for non-existent key
        value, ttl = await wrapper.ttl(collection="test", key="missing")
        assert value is None
        assert ttl is None

    async def test_ttl_many_caches_from_primary(self):
        """Test that ttl_many retrieves from primary and caches results."""
        primary_store = MemoryStore()
        cache_store = MemoryStore()
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put data in primary with TTL
        await primary_store.put(collection="test", key="k1", value={"v": "1"}, ttl=100)
        await primary_store.put(collection="test", key="k2", value={"v": "2"}, ttl=200)

        # Call ttl_many - should get from primary and cache
        results = await wrapper.ttl_many(collection="test", keys=["k1", "k2"])
        assert results[0][0] == {"v": "1"}
        assert results[1][0] == {"v": "2"}

        # Verify in cache
        assert await cache_store.get(collection="test", key="k1") == {"v": "1"}
        assert await cache_store.get(collection="test", key="k2") == {"v": "2"}

    async def test_ttl_many_returns_cached_values(self):
        """Test that ttl_many returns cached values when available."""
        primary_store = MemoryStore()
        cache_store = MemoryStore()
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put data in cache
        await cache_store.put(collection="test", key="k1", value={"v": "cached1"}, ttl=100)
        await cache_store.put(collection="test", key="k2", value={"v": "cached2"}, ttl=200)

        # Call ttl_many - should return cached values
        results = await wrapper.ttl_many(collection="test", keys=["k1", "k2"])
        assert results[0][0] == {"v": "cached1"}
        assert results[1][0] == {"v": "cached2"}
