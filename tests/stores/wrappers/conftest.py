"""Test configuration for protocol-only wrappers."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from kv_store_adapter.types import KVStoreProtocol


class BaseProtocolTests(ABC):
    """Base test class for KVStoreProtocol implementations."""

    @pytest.fixture
    @abstractmethod
    async def store(self) -> "KVStoreProtocol": ...

    async def test_empty_get(self, store: "KVStoreProtocol"):
        """Tests that the get method returns None from an empty store."""
        assert await store.get(collection="test", key="test") is None

    async def test_empty_put(self, store: "KVStoreProtocol"):
        """Tests that the put method does not raise an exception when called on a new store."""
        await store.put(collection="test", key="test", value={"test": "test"})

    async def test_empty_exists(self, store: "KVStoreProtocol"):
        """Tests that the exists method returns False from an empty store."""
        assert await store.exists(collection="test", key="test") is False

    async def test_empty_delete(self, store: "KVStoreProtocol"):
        """Tests that the delete method returns False from an empty store."""
        assert await store.delete(collection="test", key="test") is False

    async def test_get_put_get_put_delete_get(self, store: "KVStoreProtocol"):
        """Tests that the get, put, get, put, delete, and get methods work together."""
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

        await store.put(collection="test", key="test", value={"test": "test_2"})

        assert await store.get(collection="test", key="test") == {"test": "test_2"}
        assert await store.delete(collection="test", key="test")
        assert await store.get(collection="test", key="test") is None

    async def test_exists_functionality(self, store: "KVStoreProtocol"):
        """Tests that the exists method works correctly."""
        assert await store.exists(collection="test", key="test") is False
        
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.exists(collection="test", key="test") is True
        
        await store.delete(collection="test", key="test")
        assert await store.exists(collection="test", key="test") is False

    async def test_multiple_collections(self, store: "KVStoreProtocol"):
        """Tests that multiple collections work independently."""
        await store.put(collection="test_one", key="test", value={"test": "test_one"})
        await store.put(collection="test_two", key="test", value={"test": "test_two"})

        assert await store.get(collection="test_one", key="test") == {"test": "test_one"}
        assert await store.get(collection="test_two", key="test") == {"test": "test_two"}
        
        assert await store.exists(collection="test_one", key="test") is True
        assert await store.exists(collection="test_two", key="test") is True

    async def test_multiple_keys(self, store: "KVStoreProtocol"):
        """Tests that multiple keys work independently in the same collection."""
        await store.put(collection="test", key="key_one", value={"test": "value_one"})
        await store.put(collection="test", key="key_two", value={"test": "value_two"})

        assert await store.get(collection="test", key="key_one") == {"test": "value_one"}
        assert await store.get(collection="test", key="key_two") == {"test": "value_two"}
        
        assert await store.exists(collection="test", key="key_one") is True
        assert await store.exists(collection="test", key="key_two") is True

        assert await store.delete(collection="test", key="key_one") is True
        assert await store.exists(collection="test", key="key_one") is False
        assert await store.exists(collection="test", key="key_two") is True