import tempfile
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from kv_store_adapter.stores.disk.store import DiskStore
from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.stores.wrappers.passthrough_cache import PassthroughCacheWrapper
from tests.stores.conftest import BaseStoreTests

DISK_STORE_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB


class TestPrefixCollectionWrapper(BaseStoreTests):
    @pytest.fixture
    async def primary_store(self) -> AsyncGenerator[DiskStore, None]:
        with tempfile.TemporaryDirectory() as temp_dir:
            yield DiskStore(path=temp_dir, size_limit=DISK_STORE_SIZE_LIMIT)

    @pytest.fixture
    async def cache_store(self) -> MemoryStore:
        return MemoryStore()

    @override
    @pytest.fixture
    async def store(self, primary_store: DiskStore, cache_store: MemoryStore) -> PassthroughCacheWrapper:
        return PassthroughCacheWrapper(primary_store=primary_store, cache_store=cache_store)
