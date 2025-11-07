import contextlib
import json
from collections.abc import AsyncGenerator

import pytest
from aiomcache import Client
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memcached import MemcachedStore, MemcachedV1KeySanitizationStrategy
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Memcached test configuration
MEMCACHED_HOST = "localhost"
MEMCACHED_PORT = 11211
MEMCACHED_CONTAINER_PORT = 11211

WAIT_FOR_MEMCACHED_TIMEOUT = 30

MEMCACHED_VERSIONS_TO_TEST = [
    "1.6.0-alpine",  # Released Mar 2020
    "1.6.39-alpine",  # Released Sep 2025
]


async def ping_memcached() -> bool:
    client = Client(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
    try:
        await client.stats()
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await client.close()


class MemcachedFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestMemcachedStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=MEMCACHED_VERSIONS_TO_TEST)
    async def setup_memcached(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(f"memcached-test-{version}", f"memcached:{version}", {str(MEMCACHED_CONTAINER_PORT): MEMCACHED_PORT}):
            if not await async_wait_for_true(bool_fn=ping_memcached, tries=WAIT_FOR_MEMCACHED_TIMEOUT, wait_time=1):
                msg = f"Memcached {version} failed to start"
                raise MemcachedFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_memcached: None) -> MemcachedStore:
        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        _ = await store._client.flush_all()  # pyright: ignore[reportPrivateUsage]
        return store

    @pytest.fixture
    async def sanitizing_store(self, setup_memcached: None) -> MemcachedStore:
        store = MemcachedStore(
            host=MEMCACHED_HOST,
            port=MEMCACHED_PORT,
            key_sanitization_strategy=MemcachedV1KeySanitizationStrategy(),
        )
        _ = await store._client.flush_all()  # pyright: ignore[reportPrivateUsage]
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: MemcachedStore, sanitizing_store: MemcachedStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long collection name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_long_key_name(self, store: MemcachedStore, sanitizing_store: MemcachedStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long key name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})

        await sanitizing_store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}

    @pytest.fixture
    async def memcached_client(self, store: MemcachedStore) -> Client:
        return store._client  # pyright: ignore[reportPrivateUsage]

    async def test_value_stored(self, store: MemcachedStore, memcached_client: Client):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        value = await memcached_client.get(key=b"test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        value = await memcached_client.get(key=b"test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "expires_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )
