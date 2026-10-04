import json
import warnings
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from redis.asyncio.client import Redis
from testcontainers.core.container import DockerContainer
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
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
    return store._client


class TestRedisStoreUsername:
    """Regression tests for threading `username` through to the underlying Redis client.

    These construct a client without connecting (redis-py connects lazily), so they don't need Docker.
    """

    def test_username_passed_through_host_port_path(self):
        store = RedisStore(host="localhost", port=6379, db=0, username="alice")
        connection_kwargs: dict[str, Any] = get_client_from_store(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            store=store
        ).connection_pool.connection_kwargs
        assert connection_kwargs.get("username") == "alice"

    def test_username_passed_through_url_path(self):
        store = RedisStore(url="redis://bob:secret@localhost:6379/0")
        connection_kwargs: dict[str, Any] = get_client_from_store(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            store=store
        ).connection_pool.connection_kwargs
        assert connection_kwargs.get("username") == "bob"


class TestRedisStoreTTLCommands:
    async def test_put_uses_set_with_expiry(self, monkeypatch: pytest.MonkeyPatch):
        client = Redis(host="localhost", decode_responses=True)
        set_mock = AsyncMock(return_value=True)
        monkeypatch.setattr(client, "set", set_mock)
        store = RedisStore(client=client)

        await store.put(collection="test", key="single", value={"value": 1}, ttl=30)

        set_mock.assert_awaited_once()
        await_args = set_mock.await_args
        assert await_args is not None
        assert await_args.kwargs["name"] == "test::single"
        assert await_args.kwargs["ex"] > 0
        await client.aclose()

    async def test_put_many_uses_set_with_expiry(self, monkeypatch: pytest.MonkeyPatch):
        client = Redis(host="localhost", decode_responses=True)
        pipeline = MagicMock()
        pipeline.execute = AsyncMock(return_value=[])
        monkeypatch.setattr(client, "pipeline", MagicMock(return_value=pipeline))
        store = RedisStore(client=client)

        await store.put_many(
            collection="test",
            keys=["first", "second"],
            values=[{"value": 1}, {"value": 2}],
            ttl=30,
        )

        assert pipeline.set.call_count == 2
        assert all(call.kwargs["ex"] > 0 for call in pipeline.set.call_args_list)
        pipeline.execute.assert_awaited_once_with()
        await client.aclose()


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestRedisStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=REDIS_VERSIONS_TO_TEST)
    def redis_container(self, request: pytest.FixtureRequest):
        version = request.param
        container = DockerContainer(image=f"redis:{version}")
        container.with_exposed_ports(6379)
        with container:
            yield container

    @pytest.fixture(scope="module")
    def redis_host(self, redis_container: DockerContainer) -> str:
        return redis_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def redis_port(self, redis_container: DockerContainer) -> int:
        return int(redis_container.get_exposed_port(6379))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_redis(self, redis_container: DockerContainer, redis_host: str, redis_port: int) -> None:
        from key_value.aio.stores.redis.store import _create_redis_client

        async def ping_redis() -> bool:
            client = _create_redis_client(host=redis_host, port=redis_port, db=REDIS_DB)
            try:
                return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportGeneralTypeIssues]
            except Exception:
                return False
            finally:
                await client.aclose()

        if not await async_wait_for_true(bool_fn=ping_redis, tries=WAIT_FOR_REDIS_TIMEOUT, wait_time=1):
            msg = "Redis failed to start"
            raise RedisFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_redis: None, redis_host: str, redis_port: int) -> RedisStore:
        """Create a Redis store for testing."""
        # Create the store with test database
        redis_store = RedisStore(host=redis_host, port=redis_port, db=REDIS_DB)
        _ = await get_client_from_store(store=redis_store).flushdb()  # pyright: ignore[reportUnknownMemberType]
        return redis_store

    @pytest.fixture
    def redis_client(self, store: RedisStore) -> Redis:
        return get_client_from_store(store=store)

    async def test_redis_url_connection(self, setup_redis: None, redis_host: str, redis_port: int):
        """Test Redis store creation with URL."""
        redis_url = f"redis://{redis_host}:{redis_port}/{REDIS_DB}"
        store = RedisStore(url=redis_url)
        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
        await store.put(collection="test", key="url_test", value={"test": "value"})
        result = await store.get(collection="test", key="url_test")
        assert result == {"test": "value"}

    async def test_redis_client_connection(self, setup_redis: None, redis_host: str, redis_port: int):
        """Test Redis store creation with existing client."""
        from key_value.aio.stores.redis.store import _create_redis_client

        client = _create_redis_client(host=redis_host, port=redis_port, db=REDIS_DB)
        store = RedisStore(client=client)

        _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
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

    async def test_ttl_writes_do_not_use_deprecated_redis_commands(self, store: RedisStore, redis_client: Redis):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            await store.put(collection="test", key="single_ttl", value={"test": "single"}, ttl=30)
            await store.put_many(
                collection="test",
                keys=["bulk_ttl_1", "bulk_ttl_2"],
                values=[{"test": "bulk_1"}, {"test": "bulk_2"}],
                ttl=30,
            )

        assert await store.get(collection="test", key="single_ttl") == {"test": "single"}
        assert await store.get_many(collection="test", keys=["bulk_ttl_1", "bulk_ttl_2"]) == [
            {"test": "bulk_1"},
            {"test": "bulk_2"},
        ]
        assert await redis_client.ttl("test::single_ttl") > 0
        assert await redis_client.ttl("test::bulk_ttl_1") > 0
        assert await redis_client.ttl("test::bulk_ttl_2") > 0

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
