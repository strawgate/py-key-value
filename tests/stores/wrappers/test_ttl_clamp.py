import pytest
from dirty_equals import IsFloat
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.ttl_clamp import TTLClampWrapper
from tests.stores.base import BaseStoreTests


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize(("missing_ttl", "expected"), [(5, 50), (75, 75), (1000, 100), (None, None)])
async def test_missing_ttl_fallback_respects_bounds_and_preserves_none(bulk: bool, missing_ttl: float | None, expected: float | None):
    primary = MemoryStore()
    wrapper = TTLClampWrapper(primary, min_ttl=50, max_ttl=100, missing_ttl=missing_ttl)
    if bulk:
        await wrapper.put_many(["key"], [{"value": 1}])
    else:
        await wrapper.put("key", {"value": 1})

    value, ttl = await primary.ttl("key")
    assert value == {"value": 1}
    if expected is None:
        assert ttl is None
    else:
        assert ttl == IsFloat(approx=expected)


class TestTTLClampWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> TTLClampWrapper:
        return TTLClampWrapper(key_value=memory_store, min_ttl=0, max_ttl=100)

    async def test_put_below_min_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(key_value=memory_store, min_ttl=50, max_ttl=100)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=5)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        value, ttl = await ttl_clamp_store.ttl(collection="test", key="test")
        assert value is not None
        assert ttl is not None
        assert ttl == IsFloat(approx=50)

    async def test_put_above_max_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(key_value=memory_store, min_ttl=0, max_ttl=100)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=1000)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        value, ttl = await ttl_clamp_store.ttl(collection="test", key="test")
        assert value is not None
        assert ttl is not None
        assert ttl == IsFloat(approx=100)

    async def test_put_missing_ttl(self, memory_store: MemoryStore):
        ttl_clamp_store: TTLClampWrapper = TTLClampWrapper(key_value=memory_store, min_ttl=0, max_ttl=100, missing_ttl=50)

        await ttl_clamp_store.put(collection="test", key="test", value={"test": "test"}, ttl=None)
        assert await ttl_clamp_store.get(collection="test", key="test") is not None

        value, ttl = await ttl_clamp_store.ttl(collection="test", key="test")
        assert value is not None
        assert ttl is not None

        assert ttl == IsFloat(approx=50)
