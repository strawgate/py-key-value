from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.fallback import FallbackWrapper
from tests.stores.base import BaseStoreTests


class FailingStore(MemoryStore):
    """A store that always fails."""

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None):
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def put_many(
        self, keys: Sequence[str], values: Sequence[Mapping[str, Any]], *, collection: str | None = None, ttl: SupportsFloat | None = None
    ) -> None:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        msg = "Primary store unavailable"
        raise ConnectionError(msg)


class TestFallbackWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> FallbackWrapper:
        fallback_store = MemoryStore()
        return FallbackWrapper(primary_key_value=memory_store, fallback_key_value=fallback_store)

    async def test_fallback_on_primary_failure(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store)

        # Put data in fallback store directly
        await fallback_store.put(collection="test", key="test", value={"test": "fallback_value"})

        # Should fall back to secondary store
        result = await wrapper.get(collection="test", key="test")
        assert result == {"test": "fallback_value"}

    async def test_primary_success_no_fallback(self):
        primary_store = MemoryStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store)

        # Put data in primary store
        await primary_store.put(collection="test", key="test", value={"test": "primary_value"})

        # Put different data in fallback store
        await fallback_store.put(collection="test", key="test", value={"test": "fallback_value"})

        # Should use primary store
        result = await wrapper.get(collection="test", key="test")
        assert result == {"test": "primary_value"}

    async def test_write_to_fallback_disabled(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store, write_to_fallback=False)

        # Writes should fail without falling back
        with pytest.raises(ConnectionError):
            await wrapper.put(collection="test", key="test", value={"test": "value"})

    async def test_write_to_fallback_enabled(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store, write_to_fallback=True)

        # Writes should fall back to secondary
        await wrapper.put(collection="test", key="test", value={"test": "value"})

        # Verify it was written to fallback
        result = await fallback_store.get(collection="test", key="test")
        assert result == {"test": "value"}

    async def test_fallback_get_many(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store)

        # Put data in fallback store
        await fallback_store.put(collection="test", key="k1", value={"v": "1"})
        await fallback_store.put(collection="test", key="k2", value={"v": "2"})

        # Should fall back for get_many
        result = await wrapper.get_many(collection="test", keys=["k1", "k2"])
        assert result == [{"v": "1"}, {"v": "2"}]

    async def test_fallback_ttl(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store)

        # Put data in fallback store with TTL
        await fallback_store.put(collection="test", key="test", value={"v": "1"}, ttl=100)

        # Should fall back for ttl
        value, ttl = await wrapper.ttl(collection="test", key="test")
        assert value == {"v": "1"}
        assert ttl is not None

    async def test_fallback_ttl_many(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store)

        # Put data in fallback store
        await fallback_store.put(collection="test", key="k1", value={"v": "1"}, ttl=100)
        await fallback_store.put(collection="test", key="k2", value={"v": "2"}, ttl=200)

        # Should fall back for ttl_many
        results = await wrapper.ttl_many(collection="test", keys=["k1", "k2"])
        assert results[0][0] == {"v": "1"}
        assert results[1][0] == {"v": "2"}

    async def test_fallback_put_many_enabled(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store, write_to_fallback=True)

        # Should fall back for put_many
        await wrapper.put_many(collection="test", keys=["k1", "k2"], values=[{"v": "1"}, {"v": "2"}])

        # Verify in fallback
        assert await fallback_store.get(collection="test", key="k1") == {"v": "1"}
        assert await fallback_store.get(collection="test", key="k2") == {"v": "2"}

    async def test_fallback_delete_enabled(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store, write_to_fallback=True)

        # Put data in fallback
        await fallback_store.put(collection="test", key="test", value={"v": "1"})

        # Should fall back for delete
        result = await wrapper.delete(collection="test", key="test")
        assert result is True
        assert await fallback_store.get(collection="test", key="test") is None

    async def test_fallback_delete_many_enabled(self):
        primary_store = FailingStore()
        fallback_store = MemoryStore()
        wrapper = FallbackWrapper(primary_key_value=primary_store, fallback_key_value=fallback_store, write_to_fallback=True)

        # Put data in fallback
        await fallback_store.put(collection="test", key="k1", value={"v": "1"})
        await fallback_store.put(collection="test", key="k2", value={"v": "2"})

        # Should fall back for delete_many
        result = await wrapper.delete_many(collection="test", keys=["k1", "k2"])
        assert result == 2
