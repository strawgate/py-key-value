import contextlib
import json
from collections.abc import Generator

import pytest
from aiomcache import Client
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from testcontainers.memcached import MemcachedContainer
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memcached import MemcachedStore, MemcachedV1KeySanitizationStrategy
from key_value.aio.stores.memcached.store import (
    MAX_ABSOLUTE_EXPTIME,
    MAX_RELATIVE_EXPTIME,
    _create_memcached_client,
    _exptime_from_ttl,
    _memcached_close,
    _memcached_flush_all,
    _memcached_stats,
)
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Memcached test configuration
MEMCACHED_CONTAINER_PORT = 11211

WAIT_FOR_MEMCACHED_TIMEOUT = 30

MEMCACHED_VERSIONS_TO_TEST = [
    "1.6.0-alpine",  # Released Mar 2020
    "1.6.39-alpine",  # Released Sep 2025
]


async def ping_memcached(host: str, port: int) -> bool:
    client = _create_memcached_client(host=host, port=port)
    try:
        await _memcached_stats(client=client)
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await _memcached_close(client=client)


class MemcachedFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestMemcachedStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=MEMCACHED_VERSIONS_TO_TEST)
    def memcached_container(self, request: pytest.FixtureRequest) -> Generator[MemcachedContainer, None, None]:
        version = request.param
        with MemcachedContainer(image=f"memcached:{version}") as container:
            yield container

    @pytest.fixture(scope="module")
    def memcached_host(self, memcached_container: MemcachedContainer) -> str:
        return memcached_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def memcached_port(self, memcached_container: MemcachedContainer) -> int:
        return int(memcached_container.get_exposed_port(MEMCACHED_CONTAINER_PORT))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_memcached(self, memcached_container: MemcachedContainer, memcached_host: str, memcached_port: int) -> None:
        if not await async_wait_for_true(
            bool_fn=lambda: ping_memcached(memcached_host, memcached_port), tries=WAIT_FOR_MEMCACHED_TIMEOUT, wait_time=1
        ):
            msg = "Memcached failed to start"
            raise MemcachedFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_memcached: None, memcached_host: str, memcached_port: int) -> MemcachedStore:
        store = MemcachedStore(host=memcached_host, port=memcached_port)
        _ = await _memcached_flush_all(client=store._client)
        return store

    @pytest.fixture
    async def sanitizing_store(self, setup_memcached: None, memcached_host: str, memcached_port: int) -> MemcachedStore:
        store = MemcachedStore(
            host=memcached_host,
            port=memcached_port,
            key_sanitization_strategy=MemcachedV1KeySanitizationStrategy(),
        )
        _ = await _memcached_flush_all(client=store._client)
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
        return store._client

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

    async def test_put_ttl_over_30_days(self, store: MemcachedStore):
        """A TTL over 30 days is stored and readable, not treated as an expired timestamp (#386)."""
        thirty_one_days = 31 * 24 * 60 * 60

        await store.put(collection="test", key="long_ttl", value={"v": 1}, ttl=thirty_one_days)
        await store.put_many(collection="test", keys=["long_ttl_many"], values=[{"v": 2}], ttl=thirty_one_days)

        assert await store.get(collection="test", key="long_ttl") == {"v": 1}
        assert await store.get(collection="test", key="long_ttl_many") == {"v": 2}

        _, ttl = await store.ttl(collection="test", key="long_ttl")
        assert ttl is not None
        assert ttl > thirty_one_days - 60


@pytest.mark.parametrize(
    ("ttl", "expected"),
    [
        (None, 0),
        (0.5, 1),
        (-5.0, 1),
        (60.9, 60),
        (float(MAX_RELATIVE_EXPTIME), MAX_RELATIVE_EXPTIME),
        (MAX_RELATIVE_EXPTIME + 0.5, 1_000_000_000 + MAX_RELATIVE_EXPTIME + 1),
        (40 * 365 * 24 * 60 * 60, MAX_ABSOLUTE_EXPTIME),
        (MAX_RELATIVE_EXPTIME + 1.0, 1_000_000_000 + MAX_RELATIVE_EXPTIME + 1),
        (31 * 24 * 60 * 60 + 0.25, 1_000_000_000 + 31 * 24 * 60 * 60 + 1),
    ],
)
def test_exptime_from_ttl(ttl: float | None, expected: int):
    assert _exptime_from_ttl(ttl=ttl, now=1_000_000_000.0) == expected
