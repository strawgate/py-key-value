import json
from typing import Any

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from redis.asyncio.client import Redis
from testcontainers.redis import RedisContainer
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.redis import RedisStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# Redis test configuration
REDIS_DB = 15  # Use a separate database for tests

WAIT_FOR_REDIS_TIMEOUT = 30

REDIS_VERSIONS_TO_TEST = [
    "4.0.0",
    "7.0.0",
]


class RedisFailedToStartError(Exception):
    pass


def get_client_from_store(store: RedisStore) -> Redis:
    return store._client  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestRedisStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=REDIS_VERSIONS_TO_TEST)
    def redis_container(self, request: pytest.FixtureRequest):
        version = request.param
        container = RedisContainer(image=f"redis:{version}")
        container.start()
        yield container
        container.stop()

    @pytest.fixture(scope="session")
    def redis_host(self, redis_container: RedisContainer) -> str:
        return redis_container.get_container_host_ip()

    @pytest.fixture(scope="session")
    def redis_port(self, redis_container: RedisContainer) -> int:
        return int(redis_container.get_exposed_port(6379))

    @pytest.fixture(scope="session")
    async def setup_redis(self, redis_container: RedisContainer, redis_host: str, redis_port: int) -> None:
        async def ping_redis() -> bool:
            client: Redis = Redis(host=redis_host, port=redis_port, db=REDIS_DB, decode_responses=True)
            try:
                return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportAny, reportReturnType, reportUnknownVariableType, reportGeneralTypeIssues]
            except Exception:
                return False

        if not await async_wait_for_true(bool_fn=ping_redis, tries=30, wait_time=1):
            msg = "Redis failed to start"
            raise RedisFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_redis: None, redis_host: str, redis_port: int) -> RedisStore:
        """Create a Redis store for testing."""
        # Create the store with test database
        redis_store = RedisStore(host=redis_host, port=redis_port, db=REDIS_DB)
        _ = await get_client_from_store(store=redis_store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        return redis_store

    @pytest.fixture
    def redis_client(self, store: RedisStore) -> Redis:
        return get_client_from_store(store=store)

    async def test_redis_url_connection(self, setup_redis: None, redis_host: str, redis_port: int):
        """Test Redis store creation with URL."""
        redis_url = f"redis://{redis_host}:{redis_port}/{REDIS_DB}"
        store = RedisStore(url=redis_url)
        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_redis_client_connection(self, setup_redis: None, redis_host: str, redis_port: int):
        """Test Redis store creation with existing client."""
        from redis.asyncio import Redis

        client = Redis(host=redis_host, port=redis_port, db=REDIS_DB, decode_responses=True)
        store = RedisStore(client=client)

        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType, reportAny]
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    async def test_redis_document_format(self, store: RedisStore, redis_client: Redis):
        """Test Redis store document format."""
        await store.put(collection="test", key="document_format_test_1", value={"test_1": "value_1"})
        await store.put(collection="test", key="document_format_test_2", value={"test_2": "value_2"}, ttl=10)

        raw_documents: Any = await redis_client.mget(keys=["test::document_format_test_1", "test::document_format_test_2"])
        raw_documents_dicts: list[dict[str, Any]] = [json.loads(raw_document) for raw_document in raw_documents]
        assert raw_documents_dicts == snapshot(
            [
                {
                    "collection": "test",
                    "created_at": IsDatetime(iso_string=True),
                    "key": "document_format_test_1",
                    "value": {"test_1": "value_1"},
                    "version": 1,
                },
                {
                    "collection": "test",
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "key": "document_format_test_2",
                    "value": {"test_2": "value_2"},
                    "version": 1,
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
                    "collection": "test",
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "key": "document_format_test_3",
                    "value": {"test_3": "value_3"},
                    "version": 1,
                },
                {
                    "collection": "test",
                    "created_at": IsDatetime(iso_string=True),
                    "expires_at": IsDatetime(iso_string=True),
                    "key": "document_format_test_4",
                    "value": {"test_4": "value_4"},
                    "version": 1,
                },
            ]
        )

        await store.put(collection="test", key="document_format_test", value={"test": "value"}, ttl=10)
        raw_document: Any = await redis_client.get(name="test::document_format_test")
        raw_document_dict = json.loads(raw_document)
        assert raw_document_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "expires_at": IsDatetime(iso_string=True),
                "key": "document_format_test",
                "value": {"test": "value"},
                "version": 1,
            }
        )

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
