import contextlib
from collections.abc import AsyncGenerator

import pytest
from aiomcache import Client
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memcached import MemcachedStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Memcached test configuration
MEMCACHED_HOST = "localhost"
MEMCACHED_PORT = 11211
MEMCACHED_IMAGE = "memcached:1.6"

WAIT_FOR_MEMCACHED_TIMEOUT = 30


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
    @pytest.fixture(autouse=True, scope="session")
    async def setup_memcached(self) -> AsyncGenerator[None, None]:
        with docker_container("memcached-test", MEMCACHED_IMAGE, {"11211": MEMCACHED_PORT}):
            if not await async_wait_for_true(bool_fn=ping_memcached, tries=30, wait_time=1):
                msg = "Memcached failed to start"
                raise MemcachedFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_memcached: MemcachedStore) -> MemcachedStore:
        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        _ = await store._client.flush_all()  # pyright: ignore[reportPrivateUsage]
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
