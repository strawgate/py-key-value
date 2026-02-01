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
