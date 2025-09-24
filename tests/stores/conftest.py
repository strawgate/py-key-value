import asyncio
import hashlib
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from dirty_equals import IsDatetime, IsList

from kv_store_adapter.stores.base.unmanaged import BaseKVStore

if TYPE_CHECKING:
    from kv_store_adapter.types import TTLInfo


def now() -> datetime:
    return datetime.now(tz=timezone.utc)


def now_plus(seconds: int) -> datetime:
    return now() + timedelta(seconds=seconds)


class BaseStoreTests(ABC):
    async def eventually_consistent(self) -> None:  # noqa: B027
        """Subclasses can override this to wait for eventually consistent operations."""

    @pytest.fixture
    @abstractmethod
    async def store(self) -> BaseKVStore | AsyncGenerator[BaseKVStore, None]: ...

    async def test_empty_get(self, store: BaseKVStore):
        """Tests that the get method returns None from an empty store."""
        assert await store.get(collection="test", key="test") is None

    async def test_empty_set(self, store: BaseKVStore):
        """Tests that the set method does not raise an exception when called on a new store."""
        await store.put(collection="test", key="test", value={"test": "test"})

    async def test_empty_exists(self, store: BaseKVStore):
        """Tests that the exists method returns False from an empty store."""
        assert await store.exists(collection="test", key="test") is False

    async def test_empty_ttl(self, store: BaseKVStore):
        """Tests that the ttl method returns None from an empty store."""
        assert await store.ttl(collection="test", key="test") is None

    async def test_empty_keys(self, store: BaseKVStore):
        """Tests that the keys method returns an empty list from an empty store."""
        assert await store.keys(collection="test") == []

    async def test_empty_clear_collection(self, store: BaseKVStore):
        """Tests that the clear collection method returns 0 from an empty store."""
        assert await store.clear_collection(collection="test") == 0

    async def test_empty_list_collections(self, store: BaseKVStore):
        """Tests that the list collections method returns an empty list from an empty store."""
        assert await store.list_collections() == []

    async def test_empty_cull(self, store: BaseKVStore):
        """Tests that the cull method does not raise an exception when called on an empty store."""
        await store.cull()

    async def test_get_set_get(self, store: BaseKVStore):
        assert await store.get(collection="test", key="test") is None
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

    async def test_set_exists_delete_exists(self, store: BaseKVStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.exists(collection="test", key="test")
        assert await store.delete(collection="test", key="test")
        assert await store.exists(collection="test", key="test") is False

    async def test_get_set_get_delete_get(self, store: BaseKVStore):
        """Tests that the get, set, delete, and get methods work together to store and retrieve a value from an empty store."""

        assert await store.ttl(collection="test", key="test") is None

        await store.put(collection="test", key="test", value={"test": "test"})

        assert await store.get(collection="test", key="test") == {"test": "test"}

        assert await store.delete(collection="test", key="test")

        assert await store.get(collection="test", key="test") is None

    async def test_get_set_keys_delete_keys_get(self, store: BaseKVStore):
        """Tests that the get, set, keys, delete, keys, clear, and get methods work together to store and retrieve a value from an empty store."""

        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}
        assert await store.keys(collection="test") == ["test"]

        assert await store.delete(collection="test", key="test")

        await self.eventually_consistent()
        assert await store.keys(collection="test") == []

        assert await store.get(collection="test", key="test") is None

    async def test_get_set_get_set_delete_get(self, store: BaseKVStore):
        """Tests that the get, set, get, set, delete, and get methods work together to store and retrieve a value from an empty store."""
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

        await store.put(collection="test", key="test", value={"test": "test_2"})

        assert await store.get(collection="test", key="test") == {"test": "test_2"}
        assert await store.delete(collection="test", key="test")
        assert await store.get(collection="test", key="test") is None

    async def test_set_ttl_get_ttl(self, store: BaseKVStore):
        """Tests that the set and get ttl methods work together to store and retrieve a ttl from an empty store."""
        await store.put(collection="test", key="test", value={"test": "test"}, ttl=100)
        ttl_info: TTLInfo | None = await store.ttl(collection="test", key="test")
        assert ttl_info is not None
        assert ttl_info.ttl == 100

        assert ttl_info.created_at is not None
        assert ttl_info.created_at == IsDatetime(approx=now())
        assert ttl_info.expires_at is not None
        assert ttl_info.expires_at == IsDatetime(approx=now_plus(seconds=100))

        assert ttl_info.collection == "test"
        assert ttl_info.key == "test"

    async def test_list_collections(self, store: BaseKVStore):
        """Tests that the list collections method returns an empty list from an empty store."""
        assert await store.list_collections() == []

    async def test_cull(self, store: BaseKVStore):
        """Tests that the cull method does not raise an exception when called on an empty store."""
        await store.cull()

    async def test_set_set_list_collections(self, store: BaseKVStore):
        """Tests that a list collections call after adding keys to two distinct collections returns the correct collections."""
        await store.put(collection="test_one", key="test_one", value={"test": "test"})
        await self.eventually_consistent()
        assert await store.list_collections() == IsList("test_one", check_order=False)

        assert await store.get(collection="test_one", key="test_one") == {"test": "test"}
        await self.eventually_consistent()
        assert await store.list_collections() == IsList("test_one", check_order=False)

        await store.put(collection="test_two", key="test_two", value={"test": "test"})
        await self.eventually_consistent()
        assert await store.list_collections() == IsList("test_one", "test_two", check_order=False)

        assert await store.get(collection="test_two", key="test_two") == {"test": "test"}
        await self.eventually_consistent()
        assert await store.list_collections() == IsList("test_one", "test_two", check_order=False)

    async def test_set_expired_get_none(self, store: BaseKVStore):
        """Tests that a set call with a negative ttl will return None when getting the key."""
        await store.put(collection="test_collection", key="test_key", value={"test": "test"}, ttl=-100)
        assert await store.get(collection="test_collection", key="test_key") is None

    async def test_not_unbounded(self, store: BaseKVStore):
        """Tests that the store is not unbounded."""

        for i in range(5000):
            value = hashlib.sha256(f"test_{i}".encode()).hexdigest()
            await store.put(collection="test_collection", key=f"test_key_{i}", value={"test": value})

        assert await store.get(collection="test_collection", key="test_key_0") is None
        assert await store.get(collection="test_collection", key="test_key_4999") is not None

    async def test_concurrent_operations(self, store: BaseKVStore):
        """Tests that the store can handle concurrent operations."""

        async def worker(store: BaseKVStore, worker_id: int):
            for i in range(100):
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}"}

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}_2"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}_2"}

                assert await store.delete(collection="test_collection", key=f"test_{worker_id}_{i}")
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

        _ = await asyncio.gather(*[worker(store, worker_id) for worker_id in range(1)])

        assert await store.keys(collection="test_collection") == []
