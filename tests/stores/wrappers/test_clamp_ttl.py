from typing import TYPE_CHECKING

import pytest
from dirty_equals import IsDatetime
from typing_extensions import override

from kv_store_adapter.stores.memory.store import MemoryStore
from kv_store_adapter.stores.wrappers.clamp_ttl import TTLClampWrapper
from tests.stores.conftest import BaseStoreTests, now, now_plus

if TYPE_CHECKING:
    from kv_store_adapter.types import TTLInfo


class TestTTLClampWrapper(BaseStoreTests):
    @pytest.fixture
    async def memory_store(self) -> MemoryStore:
        return MemoryStore()

    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> TTLClampWrapper:
        return TTLClampWrapper(store=memory_store, min_ttl=0, max_ttl=100)

    async def test_put_below_min_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(store=memory_store, min_ttl=50, max_ttl=100)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=5)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        ttl_info: TTLInfo | None = await ttl_clamp_store.ttl(collection="test", key="test")
        assert ttl_info is not None
        assert ttl_info.ttl == 50

        assert ttl_info.created_at is not None
        assert ttl_info.created_at == IsDatetime(approx=now())

        assert ttl_info.expires_at is not None
        assert ttl_info.expires_at == IsDatetime(approx=now_plus(seconds=50))

    async def test_put_above_max_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(store=memory_store, min_ttl=0, max_ttl=100)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=1000)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        ttl_info: TTLInfo | None = await ttl_clamp_store.ttl(collection="test", key="test")
        assert ttl_info is not None
        assert ttl_info.ttl == 100

        assert ttl_info.created_at is not None
        assert ttl_info.created_at == IsDatetime(approx=now())

        assert ttl_info.expires_at is not None
        assert ttl_info.expires_at == IsDatetime(approx=now_plus(seconds=100))

    async def test_put_missing_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(store=memory_store, min_ttl=0, max_ttl=100, missing_ttl=50)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=None)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        ttl_info: TTLInfo | None = await ttl_clamp_store.ttl(collection="test", key="test")
        assert ttl_info is not None
        assert ttl_info.ttl == 50

        assert ttl_info.expires_at is not None
        assert ttl_info.expires_at == IsDatetime(approx=now_plus(seconds=50))

        assert ttl_info.created_at is not None
        assert ttl_info.created_at == IsDatetime(approx=now())
