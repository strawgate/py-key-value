from collections.abc import AsyncGenerator

import pytest
from key_value.shared.stores.wait import async_wait_for_true
from redis.asyncio.client import Redis
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.redis import RedisStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Redis test configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 15  # Use a separate database for tests

WAIT_FOR_REDIS_TIMEOUT = 30

REDIS_VERSIONS_TO_TEST = [
    "4.0.0",
    "7.0.0",
]


async def ping_redis() -> bool:
    client: Redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    try:
        return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportAny, reportReturnType, reportUnknownVariableType, reportGeneralTypeIssues]
    except Exception:
        return False


class RedisFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestRedisStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=REDIS_VERSIONS_TO_TEST)
    async def setup_redis(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container("redis-test", f"redis:{version}", {"6379": REDIS_PORT}):
            if not await async_wait_for_true(bool_fn=ping_redis, tries=30, wait_time=1):
                msg = "Redis failed to start"
                raise RedisFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_redis: RedisStore) -> RedisStore:
        """Create a Redis store for testing."""
        # Create the store with test database
        redis_store = RedisStore(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        _ = await redis_store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        return redis_store

    async def test_redis_url_connection(self):
        """Test Redis store creation with URL."""
        redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        store = RedisStore(url=redis_url)
        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_redis_client_connection(self):
        """Test Redis store creation with existing client."""
        from redis.asyncio import Redis

        client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        store = RedisStore(client=client)

        _ = await store._client.flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
