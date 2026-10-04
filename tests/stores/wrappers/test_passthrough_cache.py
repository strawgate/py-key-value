import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio.stores.disk.store import DiskStore
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.passthrough_cache import PassthroughCacheWrapper
from tests.stores.base import BaseStoreTests

DISK_STORE_SIZE_LIMIT = 100 * 1024  # 100KB


@pytest.mark.parametrize("method", ["get", "ttl", "get_many", "ttl_many"])
@pytest.mark.parametrize("cached", [False, True])
async def test_empty_dictionary_is_present_in_primary_or_cache(method: str, cached: bool):
    primary, cache = MemoryStore(), MemoryStore()
    wrapper = PassthroughCacheWrapper(primary, cache)
    source = cache if cached else primary
    await source.put("empty", {}, collection="test", ttl=60)

    if method == "get":
        assert await wrapper.get("empty", collection="test") == {}
    elif method == "get_many":
        assert await wrapper.get_many(["empty", "missing"], collection="test") == [{}, None]
    else:
        if method == "ttl":
            result = await wrapper.ttl("empty", collection="test")
        else:
            results = await wrapper.ttl_many(["empty", "missing"], collection="test")
            assert results[1] == (None, None)
            result = results[0]
        assert result[0] == {}
        assert result[1] is not None
        assert 0 < result[1] <= 60

    assert await cache.get("empty", collection="test") == {}
    assert await wrapper.get("missing", collection="test") is None
    assert await wrapper.ttl("missing", collection="test") == (None, None)


@pytest.mark.parametrize("bulk", [False, True])
async def test_cache_missing_ttl_respects_configured_maximum(bulk: bool):
    primary, cache = MemoryStore(), MemoryStore()
    wrapper = PassthroughCacheWrapper(primary, cache, maximum_ttl=60)
    await primary.put("key", {"value": 1})
    if bulk:
        assert await wrapper.get_many(["key"]) == [{"value": 1}]
    else:
        assert await wrapper.get("key") == {"value": 1}

    value, ttl = await cache.ttl("key")
    assert value == {"value": 1}
    assert ttl is not None
    assert 0 < ttl <= 60


class TestPassthroughCacheWrapper(BaseStoreTests):
    @pytest.fixture(scope="module")
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
        primary_store._cache.clear()
        return PassthroughCacheWrapper(primary_key_value=primary_store, cache_key_value=cache_store)
