import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk.store import DiskStore
from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.wrappers.passthrough_cache import PassthroughCacheWrapper
from tests.stores.conftest import BaseStoreTests

DISK_STORE_SIZE_LIMIT = 100 * 1024  # 100KB


class TestPassthroughCacheWrapper(BaseStoreTests):
    @pytest.fixture
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
        return PassthroughCacheWrapper(primary_store=primary_store, cache_store=cache_store)
