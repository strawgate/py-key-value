import asyncio
import contextlib
from collections.abc import AsyncGenerator

import pytest
from aiomcache import Client
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memcached import MemcachedStore
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, should_skip_docker_tests

# Memcached test configuration
MEMCACHED_HOST = "localhost"
MEMCACHED_PORT = 11211

WAIT_FOR_MEMCACHED_TIMEOUT = 30


async def ping_memcached() -> bool:
    client = Client(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
    try:
        _ = await client.set(b"ping", b"1", exptime=1)
        _ = await client.get(b"ping")
    except Exception:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            await client.close()


async def wait_memcached() -> bool:
    for _ in range(WAIT_FOR_MEMCACHED_TIMEOUT):
        if await ping_memcached():
            return True
        await asyncio.sleep(delay=1)
    return False


class MemcachedFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMemcachedStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_memcached(self) -> AsyncGenerator[None, None]:
        _ = await asyncio.create_subprocess_exec("docker", "stop", "memcached-test")
        _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "memcached-test")

        process = await asyncio.create_subprocess_exec(
            "docker", "run", "-d", "--name", "memcached-test", "-p", "11211:11211", "memcached:1.6-alpine"
        )
        _ = await process.wait()
        if not await wait_memcached():
            msg = "Memcached failed to start"
            raise MemcachedFailedToStartError(msg)
        try:
            yield
        finally:
            _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "memcached-test")

    @override
    @pytest.fixture
    async def store(self, setup_memcached: MemcachedStore) -> MemcachedStore:
        store = MemcachedStore(host=MEMCACHED_HOST, port=MEMCACHED_PORT)
        _ = await store._client.flush_all()  # pyright: ignore[reportPrivateUsage]
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
