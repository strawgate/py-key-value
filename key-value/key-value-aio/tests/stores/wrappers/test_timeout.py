import asyncio
from collections.abc import Sequence

import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.timeout import TimeoutWrapper
from tests.stores.base import BaseStoreTests


class SlowStore(MemoryStore):
    """A store that takes a long time to respond."""

    def __init__(self, delay: float = 1.0):
        super().__init__()
        self.delay = delay

    async def get(self, key: str, *, collection: str | None = None):
        await asyncio.sleep(self.delay)
        return await super().get(key=key, collection=collection)

    async def get_many(self, keys: Sequence[str], *, collection: str | None = None):
        await asyncio.sleep(self.delay)
        return await super().get_many(keys=keys, collection=collection)

    async def ttl(self, key: str, *, collection: str | None = None):
        await asyncio.sleep(self.delay)
        return await super().ttl(key=key, collection=collection)

    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None):
        await asyncio.sleep(self.delay)
        return await super().ttl_many(keys=keys, collection=collection)


class TestTimeoutWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> TimeoutWrapper:
        return TimeoutWrapper(key_value=memory_store, timeout=5.0)

    async def test_timeout_on_slow_operation(self):
        slow_store = SlowStore(delay=1.0)
        timeout_store = TimeoutWrapper(key_value=slow_store, timeout=0.1)

        # Should timeout
        with pytest.raises(asyncio.TimeoutError):
            await timeout_store.get(collection="test", key="test")

    async def test_no_timeout_on_fast_operation(self, memory_store: MemoryStore):
        timeout_store = TimeoutWrapper(key_value=memory_store, timeout=1.0)

        # Should succeed
        await timeout_store.put(collection="test", key="test", value={"test": "value"})
        result = await timeout_store.get(collection="test", key="test")
        assert result == {"test": "value"}

    async def test_timeout_applies_to_all_operations(self):
        slow_store = SlowStore(delay=2.0)
        timeout_store = TimeoutWrapper(key_value=slow_store, timeout=0.1)

        # All operations should timeout
        with pytest.raises(asyncio.TimeoutError):
            await timeout_store.get(collection="test", key="test")

        with pytest.raises(asyncio.TimeoutError):
            await timeout_store.get_many(collection="test", keys=["test"])

        with pytest.raises(asyncio.TimeoutError):
            await timeout_store.ttl(collection="test", key="test")

        with pytest.raises(asyncio.TimeoutError):
            await timeout_store.ttl_many(collection="test", keys=["test"])
