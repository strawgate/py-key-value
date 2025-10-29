import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from aiomcache import Client
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memcached import MemcachedStore
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


def test_managed_entry_serialization():
    """Test ManagedEntry serialization to JSON for Memcached storage."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    json_str = managed_entry.to_json()

    assert json_str == snapshot('{"value": {"test": "test"}}')

    round_trip_managed_entry = ManagedEntry.from_json(json_str=json_str)

    assert round_trip_managed_entry.value == managed_entry.value


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

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored_as_json_bytes(self, store: MemcachedStore):
        """Verify values are stored as JSON bytes in Memcached."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get raw Memcached value using the compound key format
        combo_key = store.sanitize_key(compound_key(collection="test", key="test_key"))  # pyright: ignore[reportUnknownMemberType]
        raw_value = await store._client.get(combo_key.encode("utf-8"))  # pyright: ignore[reportPrivateUsage]

        # Decode bytes to string
        assert isinstance(raw_value, bytes)
        decoded_value = raw_value.decode("utf-8")

        assert decoded_value == snapshot('{"value": {"name": "Alice", "age": 30}}')

        # Test with TTL to verify it still stores correctly
        await store.put(collection="test", key="test_key_ttl", value={"name": "Bob", "age": 25}, ttl=3600)
        combo_key_ttl = store.sanitize_key(compound_key(collection="test", key="test_key_ttl"))  # pyright: ignore[reportUnknownMemberType]
        raw_value_ttl = await store._client.get(combo_key_ttl.encode("utf-8"))  # pyright: ignore[reportPrivateUsage]

        assert isinstance(raw_value_ttl, bytes)
        decoded_value_ttl = raw_value_ttl.decode("utf-8")

        assert decoded_value_ttl == snapshot('{"value": {"name": "Bob", "age": 25}}')
