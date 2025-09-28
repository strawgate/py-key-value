import pytest

from key_value.aio.adapters.raise_on_missing import RaiseOnMissingAdapter
from key_value.aio.errors import MissingKeyError
from key_value.aio.stores.memory.store import MemoryStore


@pytest.fixture
async def store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
async def adapter(store: MemoryStore) -> RaiseOnMissingAdapter:
    return RaiseOnMissingAdapter(key_value=store)


async def test_get(adapter: RaiseOnMissingAdapter):
    await adapter.put(collection="test", key="test", value={"test": "test"})
    assert await adapter.get(collection="test", key="test") == {"test": "test"}


async def test_get_missing(adapter: RaiseOnMissingAdapter):
    with pytest.raises(MissingKeyError):
        _ = await adapter.get(collection="test", key="test", raise_on_missing=True)


async def test_get_many(adapter: RaiseOnMissingAdapter):
    await adapter.put(collection="test", key="test", value={"test": "test"})
    await adapter.put(collection="test", key="test_2", value={"test": "test_2"})
    assert await adapter.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]


async def test_get_many_missing(adapter: RaiseOnMissingAdapter):
    await adapter.put(collection="test", key="test", value={"test": "test"})
    with pytest.raises(MissingKeyError):
        _ = await adapter.get_many(collection="test", keys=["test", "test_2"], raise_on_missing=True)
