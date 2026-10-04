import pytest
from typing_extensions import override

from key_value.aio._utils.constants import DEFAULT_COLLECTION_NAME
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.statistics import StatisticsWrapper
from tests.stores.base import BaseStoreTests


@pytest.mark.parametrize("collection", [None, "", "explicit"])
async def test_statistics_preserves_default_routing_and_single_operation_counts(collection: str | None):
    primary = MemoryStore(default_collection="custom")
    wrapper = StatisticsWrapper(primary)
    await primary.put("existing", {"value": 1}, collection=collection, ttl=60)

    assert await wrapper.get("existing", collection=collection) == {"value": 1}
    assert await wrapper.get("missing", collection=collection) is None
    value, ttl = await wrapper.ttl("existing", collection=collection)
    assert value == {"value": 1}
    assert ttl is not None
    assert 0 < ttl <= 60
    assert await wrapper.ttl("missing", collection=collection) == (None, None)

    await wrapper.put("new", {"value": 2}, collection=collection)
    assert await primary.get("new", collection=collection) == {"value": 2}
    assert await wrapper.delete("new", collection=collection) is True
    assert await primary.get("new", collection=collection) is None
    assert await wrapper.delete("missing", collection=collection) is False

    # Preserve existing metric labels without substituting them into backend calls.
    label = collection or DEFAULT_COLLECTION_NAME
    assert set(wrapper.statistics.collections) == {label}
    statistics = wrapper.statistics.get_collection(label)
    assert (statistics.get.count, statistics.get.hit, statistics.get.miss) == (2, 1, 1)
    assert (statistics.ttl.count, statistics.ttl.hit, statistics.ttl.miss) == (2, 1, 1)
    assert statistics.put.count == 1
    assert (statistics.delete.count, statistics.delete.hit, statistics.delete.miss) == (2, 1, 1)


@pytest.mark.parametrize("collection", [None, "", "explicit"])
async def test_statistics_preserves_default_routing_and_bulk_operation_counts(collection: str | None):
    primary = MemoryStore(default_collection="custom")
    wrapper = StatisticsWrapper(primary)
    await primary.put("existing", {"value": 1}, collection=collection, ttl=60)

    assert await wrapper.get_many(["existing", "missing"], collection=collection) == [{"value": 1}, None]
    ttls = await wrapper.ttl_many(["existing", "missing"], collection=collection)
    assert ttls[0][0] == {"value": 1}
    assert ttls[0][1] is not None
    assert 0 < ttls[0][1] <= 60
    assert ttls[1] == (None, None)

    await wrapper.put_many(["first", "second"], [{"value": 2}, {}], collection=collection)
    assert await primary.get_many(["first", "second"], collection=collection) == [{"value": 2}, {}]
    assert await wrapper.delete_many(["first", "second", "missing"], collection=collection) == 2
    assert await primary.get_many(["first", "second"], collection=collection) == [None, None]

    label = collection or DEFAULT_COLLECTION_NAME
    assert set(wrapper.statistics.collections) == {label}
    statistics = wrapper.statistics.get_collection(label)
    assert (statistics.get.count, statistics.get.hit, statistics.get.miss) == (2, 1, 1)
    assert (statistics.ttl.count, statistics.ttl.hit, statistics.ttl.miss) == (2, 1, 1)
    assert statistics.put.count == 2
    assert (statistics.delete.count, statistics.delete.hit, statistics.delete.miss) == (3, 2, 1)


class TestStatisticsWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> StatisticsWrapper:
        return StatisticsWrapper(key_value=memory_store)
