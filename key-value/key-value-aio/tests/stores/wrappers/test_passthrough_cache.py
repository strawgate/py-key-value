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

    async def test_cache_hit_on_get(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that data is retrieved from cache when available."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put value directly in cache
        test_value = {"test": "cached"}
        await cache_store.put(collection="test", key="cached_key", value=test_value)

        # Get should return from cache (not from primary)
        result = await wrapper.get(collection="test", key="cached_key")
        assert result == test_value

    async def test_cache_miss_populates_from_primary(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that cache miss falls back to primary and populates cache."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put value directly in primary store only
        test_value = {"data": "from_primary"}
        await primary_store.put(collection="test", key="primary_key", value=test_value, ttl=60)

        # Cache should be empty initially
        cache_result = await cache_store.get(collection="test", key="primary_key")
        assert cache_result is None

        # Get through wrapper should fetch from primary
        result = await wrapper.get(collection="test", key="primary_key")
        assert result == test_value

        # Now cache should be populated
        cache_result = await cache_store.get(collection="test", key="primary_key")
        assert cache_result == test_value

    async def test_get_many_with_mixed_cache_hits(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test get_many with some values in cache and some in primary."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put some values in cache, some in primary
        await cache_store.put(collection="test", key="cached1", value={"source": "cache"})
        await primary_store.put(collection="test", key="primary1", value={"source": "primary"}, ttl=60)

        # Get many should return both
        results = await wrapper.get_many(collection="test", keys=["cached1", "primary1", "missing"])
        assert results[0] == {"source": "cache"}
        assert results[1] == {"source": "primary"}
        assert results[2] is None

        # Primary value should now be cached
        cache_result = await cache_store.get(collection="test", key="primary1")
        assert cache_result == {"source": "primary"}

    async def test_put_invalidates_cache(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that put operations clear the corresponding cache entry."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put value in cache
        await cache_store.put(collection="test", key="key", value={"version": 1})

        # Verify it's in cache
        cache_result = await cache_store.get(collection="test", key="key")
        assert cache_result is not None

        # Put through wrapper should invalidate cache
        await wrapper.put(collection="test", key="key", value={"version": 2})

        # Cache should be cleared
        cache_result = await cache_store.get(collection="test", key="key")
        assert cache_result is None

        # Primary should have new value
        primary_result = await primary_store.get(collection="test", key="key")
        assert primary_result == {"version": 2}

    async def test_put_many_invalidates_cache(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that put_many operations clear corresponding cache entries."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put values in cache
        await cache_store.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}])

        # Put many through wrapper should invalidate cache
        await wrapper.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 10}, {"v": 20}])

        # Cache should be cleared for both
        result1 = await cache_store.get(collection="test", key="k1")
        result2 = await cache_store.get(collection="test", key="k2")
        assert result1 is None
        assert result2 is None

    async def test_delete_invalidates_cache(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that delete operations clear the cache entry."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put value in both stores
        await primary_store.put(collection="test", key="key", value={"data": "value"})
        await cache_store.put(collection="test", key="key", value={"data": "value"})

        # Delete through wrapper
        result = await wrapper.delete(collection="test", key="key")
        assert result is True

        # Both should be gone
        assert await cache_store.get(collection="test", key="key") is None
        assert await primary_store.get(collection="test", key="key") is None

    async def test_delete_many_invalidates_cache(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that delete_many operations clear cache entries."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put values in both stores
        await primary_store.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}])
        await cache_store.put_many(collection="test", keys=["k1", "k2"], values=[{"v": 1}, {"v": 2}])

        # Delete many through wrapper
        deleted = await wrapper.delete_many(collection="test", keys=["k1", "k2"])
        assert deleted == 2

        # Cache should be cleared
        assert await cache_store.get(collection="test", key="k1") is None
        assert await cache_store.get(collection="test", key="k2") is None

    async def test_ttl_respects_primary_ttl(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test that TTL from primary is respected when caching."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put value in primary with TTL
        test_value = {"data": "value"}
        await primary_store.put(collection="test", key="key", value=test_value, ttl=100)

        # Access through wrapper to populate cache
        result, ttl = await wrapper.ttl(collection="test", key="key")
        assert result == test_value
        assert ttl is not None
        assert ttl <= 100

        # Cache should have the value with clamped TTL
        cached_result, cached_ttl = await cache_store.ttl(collection="test", key="key")
        assert cached_result == test_value
        assert cached_ttl is not None

    async def test_ttl_many_with_mixed_sources(self, primary_store: DiskStore, cache_store: MemoryStore) -> None:
        """Test ttl_many with values from both cache and primary."""
        wrapper = PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)

        # Put some in cache, some in primary
        await cache_store.put(collection="test", key="cached", value={"source": "cache"})
        await primary_store.put(collection="test", key="primary", value={"source": "primary"}, ttl=60)

        # ttl_many should handle both
        results = await wrapper.ttl_many(collection="test", keys=["cached", "primary", "missing"])
        assert results[0][0] == {"source": "cache"}
        assert results[1][0] == {"source": "primary"}
        assert results[2][0] is None
