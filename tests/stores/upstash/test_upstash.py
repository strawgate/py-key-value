import os
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override
from upstash_redis.asyncio import Redis

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.upstash import UpstashRedisStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

# Every collection used by the shared store tests starts with "test".
TEST_KEY_PATTERN = "test*"


async def delete_test_keys(client: Redis) -> None:
    """Delete only the keys written by these tests, so a shared database is not flushed."""
    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor, match=TEST_KEY_PATTERN, count=1000)
        if keys:
            _ = await client.delete(*keys)
        if cursor == 0:
            return


@pytest.mark.skipif(
    not (UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN),
    reason="UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN are not set",
)
class TestUpstashRedisStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture
    async def upstash_client(self) -> AsyncGenerator[Redis, None]:
        client = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)
        await delete_test_keys(client)
        yield client
        await delete_test_keys(client)
        await client.close()

    @override
    @pytest.fixture
    async def store(self, upstash_client: Redis) -> UpstashRedisStore:
        return UpstashRedisStore(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

    async def test_upstash_client_connection(self, upstash_client: Redis):
        """Test Upstash Redis store creation with an existing client."""
        store = UpstashRedisStore(client=upstash_client)
        await store.put(collection="test", key="client_test", value={"test": "value"}, ttl=10)
        assert await store.get(collection="test", key="client_test") == {"test": "value"}
        assert await upstash_client.ttl("test::client_test") > 0

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
