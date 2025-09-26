import asyncio
import hashlib
import subprocess
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import AnyHttpUrl

from kv_store_adapter.errors import SerializationError
from kv_store_adapter.stores.base import BaseStore


def now() -> datetime:
    return datetime.now(tz=timezone.utc)


def now_plus(seconds: int) -> datetime:
    return now() + timedelta(seconds=seconds)


def is_around(value: float, delta: float = 1) -> bool:
    return value - delta < value < value + delta


def detect_docker() -> bool:
    try:
        result = subprocess.run(["docker", "ps"], check=False, capture_output=True, text=True)  # noqa: S607
    except Exception:
        return False
    else:
        return result.returncode == 0


class BaseStoreTests(ABC):
    async def eventually_consistent(self) -> None:  # noqa: B027
        """Subclasses can override this to wait for eventually consistent operations."""

    @pytest.fixture
    @abstractmethod
    async def store(self) -> BaseStore | AsyncGenerator[BaseStore, None]: ...

    async def test_empty_get(self, store: BaseStore):
        """Tests that the get method returns None from an empty store."""
        assert await store.get(collection="test", key="test") is None

    async def test_empty_put(self, store: BaseStore):
        """Tests that the put method does not raise an exception when called on a new store."""
        await store.put(collection="test", key="test", value={"test": "test"})

    async def test_empty_ttl(self, store: BaseStore):
        """Tests that the ttl method returns None from an empty store."""
        assert await store.ttl(collection="test", key="test") == (None, None)

    async def test_put_serialization_errors(self, store: BaseStore):
        """Tests that the put method does not raise an exception when called on a new store."""
        with pytest.raises(SerializationError):
            await store.put(collection="test", key="test", value={"test": AnyHttpUrl("https://test.com")})

    async def test_get_put_get(self, store: BaseStore):
        assert await store.get(collection="test", key="test") is None
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

    async def test_put_many_get(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get(collection="test", key="test") == {"test": "test"}
        assert await store.get(collection="test", key="test_2") == {"test": "test_2"}

    async def test_put_many_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]

    async def test_put_put_get_many(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        await store.put(collection="test", key="test_2", value={"test": "test_2"})
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]

    async def test_put_put_get_many_missing_one(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        await store.put(collection="test", key="test_2", value={"test": "test_2"})
        assert await store.get_many(collection="test", keys=["test", "test_2", "test_3"]) == [{"test": "test"}, {"test": "test_2"}, None]

    async def test_put_get_delete_get(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}
        assert await store.delete(collection="test", key="test")
        assert await store.get(collection="test", key="test") is None

    async def test_put_many_get_many_delete_many_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 2
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [None, None]

    async def test_get_put_get_delete_get(self, store: BaseStore):
        """Tests that the get, put, delete, and get methods work together to store and retrieve a value from an empty store."""

        assert await store.get(collection="test", key="test") is None

        await store.put(collection="test", key="test", value={"test": "test"})

        assert await store.get(collection="test", key="test") == {"test": "test"}

        assert await store.delete(collection="test", key="test")

        assert await store.get(collection="test", key="test") is None

    async def test_get_put_get_put_delete_get(self, store: BaseStore):
        """Tests that the get, put, get, put, delete, and get methods work together to store and retrieve a value from an empty store."""
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

        await store.put(collection="test", key="test", value={"test": "test_2"})

        assert await store.get(collection="test", key="test") == {"test": "test_2"}
        assert await store.delete(collection="test", key="test")
        assert await store.get(collection="test", key="test") is None

    async def test_put_many_delete_delete_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]
        assert await store.delete(collection="test", key="test")
        assert await store.delete(collection="test", key="test_2")
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [None, None]

    async def test_put_ttl_get_ttl(self, store: BaseStore):
        """Tests that the put and get ttl methods work together to store and retrieve a ttl from an empty store."""
        await store.put(collection="test", key="test", value={"test": "test"}, ttl=100)
        value, ttl = await store.ttl(collection="test", key="test")

        assert value == {"test": "test"}
        assert ttl is not None
        assert ttl < 100
        assert ttl > 90

    async def test_negative_ttl(self, store: BaseStore):
        """Tests that a negative ttl will return None when getting the key."""
        await store.put(collection="test", key="test", value={"test": "test"}, ttl=-100)

    async def test_put_expired_get_none(self, store: BaseStore):
        """Tests that a put call with a negative ttl will return None when getting the key."""
        await store.put(collection="test_collection", key="test_key", value={"test": "test"}, ttl=-100)
        assert await store.get(collection="test_collection", key="test_key") is None

    async def test_long_collection_name(self, store: BaseStore):
        """Tests that a long collection name will not raise an error."""
        await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    async def test_special_characters_in_collection_name(self, store: BaseStore):
        """Tests that a special characters in the collection name will not raise an error."""
        await store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

    async def test_long_key_name(self, store: BaseStore):
        """Tests that a long key name will not raise an error."""
        await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}

    async def test_special_characters_in_key_name(self, store: BaseStore):
        """Tests that a special characters in the key name will not raise an error."""
        await store.put(collection="test_collection", key="test_key!@#$%^&*()", value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key!@#$%^&*()") == {"test": "test"}

    async def test_not_unbounded(self, store: BaseStore):
        """Tests that the store is not unbounded."""

        for i in range(5000):
            value = hashlib.sha256(f"test_{i}".encode()).hexdigest()
            await store.put(collection="test_collection", key=f"test_key_{i}", value={"test": value})

        assert await store.get(collection="test_collection", key="test_key_0") is None
        assert await store.get(collection="test_collection", key="test_key_4999") is not None

    async def test_concurrent_operations(self, store: BaseStore):
        """Tests that the store can handle concurrent operations."""

        async def worker(store: BaseStore, worker_id: int):
            for i in range(100):
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}"}

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}_2"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}_2"}

                assert await store.delete(collection="test_collection", key=f"test_{worker_id}_{i}")
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

        _ = await asyncio.gather(*[worker(store, worker_id) for worker_id in range(1)])
