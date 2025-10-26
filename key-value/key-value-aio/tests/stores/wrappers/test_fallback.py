from collections.abc import Mapping
from typing import Any, SupportsFloat

import pytest
from typing_extensions import override

from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.fallback import FallbackWrapper
from tests.stores.base import BaseStoreTests


class FailingStore(MemoryStore):
    """A store that always fails."""

    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:  # noqa: ARG002
        msg = "Primary store unavailable"
        raise ConnectionError(msg)

    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None):  # noqa: ARG002
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
