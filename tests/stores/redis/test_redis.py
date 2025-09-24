import asyncio
from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis
from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.redis import RedisStore
from tests.stores.conftest import BaseStoreTests

# Redis test configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 15  # Use a separate database for tests

WAIT_FOR_REDIS_TIMEOUT = 30

async def ping_redis() -> bool:
    client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    try:
        return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportAny]
    except Exception:
        return False


async def wait_redis() -> bool:
    # with a timeout of 10 seconds
    for _ in range(WAIT_FOR_REDIS_TIMEOUT):
        if await ping_redis():
            return True
        await asyncio.sleep(delay=1)

    return False


class RedisFailedToStartError(Exception):
    pass


class TestRedisStore(BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_redis(self) -> AsyncGenerator[None, None]:
        _ = await asyncio.create_subprocess_exec("docker", "stop", "redis-test")
        _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "redis-test")

        process = await asyncio.create_subprocess_exec("docker", "run", "-d", "--name", "redis-test", "-p", "6379:6379", "redis")
        _ = await process.wait()
        if not await wait_redis():
            msg = "Redis failed to start"
            raise RedisFailedToStartError(msg)
        try:
            yield
        finally:
            _ = await asyncio.create_subprocess_exec("docker", "rm", "-f", "redis-test")

    @override
    @pytest.fixture
    async def store(self, setup_redis: RedisStore) -> RedisStore:
        """Create a Redis store for testing."""
        # Create the store with test database
        redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        _ = await redis_store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        return redis_store

    async def test_redis_url_connection(self):
        """Test Redis store creation with URL."""
        redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        store = RedisStore(url=redis_url)
        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_redis_client_connection(self):
        """Test Redis store creation with existing client."""
        client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        store = RedisStore(client=client)

        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseKVStore): ...
