import asyncio

import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from tests.stores.base import BaseStoreTests, PutIfAbsentStoreTestMixin


class TestMemoryStore(PutIfAbsentStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore(max_entries_per_collection=500)

    async def test_seed(self):
        store = MemoryStore(max_entries_per_collection=500, seed={"test_collection": {"test_key": {"obj_key": "obj_value"}}})
        assert await store.get(key="test_key", collection="test_collection") == {"obj_key": "obj_value"}

    async def test_expired_entries_are_evicted_from_the_underlying_cache(self, store: MemoryStore):
        """get() filtering expired entries isn't enough on its own -- the raw cache entry has to
        actually go away too, or a write-once/never-reread key leaks forever. TLRUCache's default
        timer (monotonic) doesn't match the wall-clock `expires_at` this store hands it, so its
        own eviction silently never fired even though get() correctly returned None.
        """
        await store.put(collection="test", key="short_lived", value={"data": "value"}, ttl=0.2)

        for _ in range(20):
            if await store.get(collection="test", key="short_lived") is None:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("entry never expired")

        raw_cache = store._cache["test"]._cache
        assert len(raw_cache) == 0
