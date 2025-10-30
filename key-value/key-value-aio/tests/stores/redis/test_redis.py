import json
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
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


def get_client_from_store(store: RedisStore) -> Redis:
    return store._client  # pyright: ignore[reportPrivateUsage]


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
        _ = await get_client_from_store(store=redis_store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        return redis_store

    @pytest.fixture
    def redis_client(self, store: RedisStore) -> Redis:
        return get_client_from_store(store=store)

    async def test_redis_url_connection(self):
        """Test Redis store creation with URL."""
        redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
        store = RedisStore(url=redis_url)
        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_redis_client_connection(self):
        """Test Redis store creation with existing client."""
        from redis.asyncio import Redis

        client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        store = RedisStore(client=client)

        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    async def test_redis_document_format(self, store: RedisStore, redis_client: Redis):
        """Test Redis store document format."""
        await store.put(collection="test", key="document_format_test_1", value={"test_1": "value_1"})
        await store.put(collection="test", key="document_format_test_2", value={"test_2": "value_2"}, ttl=10)

        raw_documents = await redis_client.mget(keys=["test::document_format_test_1", "test::document_format_test_2"])
        raw_documents_dicts: list[dict[str, Any]] = [json.loads(raw_document) for raw_document in raw_documents]
        assert raw_documents_dicts == snapshot(
            [
                {
                    "created_at": IsDatetime(iso_string=True),
                    "value": {"test_1": "value_1"},
                },
                {
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "value": {"test_2": "value_2"},
                },
            ]
        )

        await store.put_many(
            collection="test",
            keys=["document_format_test_3", "document_format_test_4"],
            values=[{"test_3": "value_3"}, {"test_4": "value_4"}],
            ttl=10,
        )
        raw_documents = await redis_client.mget(keys=["test::document_format_test_3", "test::document_format_test_4"])
        raw_documents_dicts = [json.loads(raw_document) for raw_document in raw_documents]
        assert raw_documents_dicts == snapshot(
            [
                {
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "value": {"test_3": "value_3"},
                },
                {
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "value": {"test_4": "value_4"},
                },
            ]
        )

        await store.put(collection="test", key="document_format_test", value={"test": "value"}, ttl=10)
        raw_document = await redis_client.get(name="test::document_format_test")
        raw_document_dict = json.loads(raw_document)
        assert raw_document_dict == snapshot(
            {"created_at": IsDatetime(iso_string=True), "expires_at": IsDatetime(iso_string=True), "value": {"test": "value"}}
        )

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
