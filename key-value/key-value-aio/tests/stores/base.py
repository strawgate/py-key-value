import asyncio
import hashlib
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from dirty_equals import IsFloat
from pydantic import AnyHttpUrl

from key_value.aio._shared.errors import InvalidTTLError, SerializationError
from key_value.aio.protocols.key_value import AsyncKeyValueProtocol
from key_value.aio.stores.base import BaseContextManagerStore, BaseStore
from tests._shared_test.cases import (
    LARGE_DATA_CASES,
    NEGATIVE_SIMPLE_CASES,
    SIMPLE_CASES,
    NegativeCases,
    PositiveCases,
)
from tests.conftest import async_running_in_event_loop


class BaseStoreTests(ABC):
    async def eventually_consistent(self) -> None:  # noqa: B027
        """Subclasses can override this to wait for eventually consistent operations."""

    @pytest.fixture
    async def per_test_temp_dir(self) -> AsyncGenerator[Path, None]:
        # ignore cleanup errors on Windows
        if sys.platform == "win32":
            ignore_cleanup_errors = True
        else:
            ignore_cleanup_errors = False

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=ignore_cleanup_errors) as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    @abstractmethod
    async def store(self) -> BaseStore | AsyncGenerator[BaseStore, None]: ...

    async def test_store(self, store: BaseStore):
        """Tests that the store is a valid AsyncKeyValueProtocol."""
        assert isinstance(store, AsyncKeyValueProtocol) is True

    async def test_empty_get(self, store: BaseStore):
        """Tests that the get method returns None from an empty store."""
        assert await store.get(collection="test", key="test") is None

    async def test_empty_put(self, store: BaseStore):
        """Tests that the put method does not raise an exception when called on a new store."""
        await store.put(collection="test", key="test", value={"test": "test"})

    async def test_empty_ttl(self, store: BaseStore):
        """Tests that the ttl method returns None from an empty store."""
        ttl = await store.ttl(collection="test", key="test")
        assert ttl == (None, None)

    async def test_put_serialization_errors(self, store: BaseStore):
        """Tests that the put method raises SerializationError for non-JSON-serializable Pydantic types."""
        with pytest.raises(SerializationError):
            await store.put(collection="test", key="test", value={"test": AnyHttpUrl("https://test.com")})

    async def test_get_put_get(self, store: BaseStore):
        assert await store.get(collection="test", key="test") is None
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.get(collection="test", key="test") == {"test": "test"}

    @PositiveCases.parametrize(cases=SIMPLE_CASES)
    async def test_models_put_get(self, store: BaseStore, data: dict[str, Any], json: str, round_trip: dict[str, Any]):
        await store.put(collection="test", key="test", value=data)
        retrieved_data = await store.get(collection="test", key="test")
        assert retrieved_data is not None
        assert retrieved_data == round_trip

    @NegativeCases.parametrize(cases=NEGATIVE_SIMPLE_CASES)
    async def test_negative_models_put_get(self, store: BaseStore, data: dict[str, Any], error: type[Exception]):
        with pytest.raises(error):
            await store.put(collection="test", key="test", value=data)

    @PositiveCases.parametrize(cases=[LARGE_DATA_CASES])
    async def test_get_large_put_get(self, store: BaseStore, data: dict[str, Any], json: str, round_trip: dict[str, Any]):
        await store.put(collection="test", key="test", value=data)
        assert await store.get(collection="test", key="test") == round_trip

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

    async def test_put_many_get_get_delete_many_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 2
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [None, None]

    async def test_put_many_get_many_delete_many_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=[{"test": "test"}, {"test": "test_2"}])
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 2
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [None, None]

    async def test_put_many_tuple_get_many(self, store: BaseStore):
        await store.put_many(collection="test", keys=["test", "test_2"], values=({"test": "test"}, {"test": "test_2"}))
        assert await store.get_many(collection="test", keys=["test", "test_2"]) == [{"test": "test"}, {"test": "test_2"}]

    async def test_delete(self, store: BaseStore):
        assert await store.delete(collection="test", key="test") is False

    async def test_put_delete_delete(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.delete(collection="test", key="test")
        assert await store.delete(collection="test", key="test") is False

    async def test_delete_many(self, store: BaseStore):
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 0

    async def test_put_delete_many(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 1

    async def test_delete_many_delete_many(self, store: BaseStore):
        await store.put(collection="test", key="test", value={"test": "test"})
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 1
        assert await store.delete_many(collection="test", keys=["test", "test_2"]) == 0

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
        assert ttl == IsFloat(approx=100, delta=2), f"TTL should be ~100, but is {ttl}"

    async def test_negative_ttl(self, store: BaseStore):
        """Tests that a negative ttl will return None when getting the key."""
        with pytest.raises(InvalidTTLError):
            await store.put(collection="test", key="test", value={"test": "test"}, ttl=-100)

    async def test_put_expired_get_none(self, store: BaseStore):
        """Tests that a put call with a negative ttl will return None when getting the key."""
        await store.put(collection="test_collection", key="test_key", value={"test": "test"}, ttl=2)
        assert await store.get(collection="test_collection", key="test_key") is not None
        await asyncio.sleep(1)

        for _ in range(8):
            await asyncio.sleep(0.25)
            if await store.get(collection="test_collection", key="test_key") is None:
                # pass the test
                return

        pytest.fail("put_expired_get_none test failed, entry did not expire")

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

        for i in range(1000):
            value = hashlib.sha256(f"test_{i}".encode()).hexdigest()
            await store.put(collection="test_collection", key=f"test_key_{i}", value={"test": value})

        assert await store.get(collection="test_collection", key="test_key_0") is None
        assert await store.get(collection="test_collection", key="test_key_999") is not None

    @pytest.mark.skipif(condition=not async_running_in_event_loop(), reason="Cannot run concurrent operations outside of event loop")
    async def test_concurrent_operations(self, store: BaseStore):
        """Tests that the store can handle concurrent operations."""

        async def worker(store: BaseStore, worker_id: int):
            for i in range(5):
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}"}

                await store.put(collection="test_collection", key=f"test_{worker_id}_{i}", value={"test": f"test_{i}_2"})
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") == {"test": f"test_{i}_2"}

                assert await store.delete(collection="test_collection", key=f"test_{worker_id}_{i}")
                assert await store.get(collection="test_collection", key=f"test_{worker_id}_{i}") is None

        _ = await asyncio.gather(*[worker(store, worker_id) for worker_id in range(3)])

    async def test_minimum_put_many_get_many_performance(self, store: BaseStore):
        """Tests that the store meets minimum performance requirements."""
        keys = [f"test_{i}" for i in range(10)]
        values = [{"test": f"test_{i}"} for i in range(10)]
        await store.put_many(collection="test_collection", keys=keys, values=values)
        assert await store.get_many(collection="test_collection", keys=keys) == values

    async def test_minimum_put_many_delete_many_performance(self, store: BaseStore):
        """Tests that the store meets minimum performance requirements."""
        keys = [f"test_{i}" for i in range(10)]
        values = [{"test": f"test_{i}"} for i in range(10)]
        await store.put_many(collection="test_collection", keys=keys, values=values)
        assert await store.delete_many(collection="test_collection", keys=keys) == 10


class ContextManagerStoreTestMixin:
    @pytest.fixture(params=[True, False], ids=["with_ctx_manager", "no_ctx_manager"], autouse=True)
    async def enter_exit_store(
        self, request: pytest.FixtureRequest, store: BaseContextManagerStore, per_test_temp_dir: Path
    ) -> AsyncGenerator[BaseContextManagerStore, None]:
        context_manager = request.param  # pyright: ignore[reportAny]

        if context_manager:
            async with store:
                yield store
        else:
            yield store
            await store.close()
