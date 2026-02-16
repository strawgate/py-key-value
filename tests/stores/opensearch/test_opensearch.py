import contextlib
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from dirty_equals import IsFloat, IsStr
from inline_snapshot import snapshot
from opensearchpy import AsyncOpenSearch
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.opensearch import OpenSearchStore
from key_value.aio.stores.opensearch.store import (
    OpenSearchSerializationAdapter,
    OpenSearchV1CollectionSanitizationStrategy,
    OpenSearchV1KeySanitizationStrategy,
)
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB

OPENSEARCH_CONTAINER_PORT = 9200

WAIT_FOR_OPENSEARCH_TIMEOUT = 30

OPENSEARCH_VERSIONS_TO_TEST = [
    "2.11.0",  # Released 2023
    "2.18.0",  # Recent stable version
]


def get_opensearch_client(opensearch_url: str) -> AsyncOpenSearch:
    return AsyncOpenSearch(hosts=[opensearch_url], use_ssl=False, verify_certs=False)


async def ping_opensearch(opensearch_url: str) -> bool:
    opensearch_client: AsyncOpenSearch = get_opensearch_client(opensearch_url)

    async with opensearch_client:
        try:
            return await opensearch_client.ping()
        except Exception:
            return False


async def cleanup_opensearch_indices(opensearch_client: AsyncOpenSearch):
    with contextlib.suppress(Exception):
        indices = await opensearch_client.indices.get(index="opensearch-kv-store-e2e-test-*")
        for index in indices:
            _ = await opensearch_client.indices.delete(index=index)


class OpenSearchFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    adapter = OpenSearchSerializationAdapter()
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "version": 1,
            "value": {"flat": {"test": "test"}},
            "created_at": "2025-01-01T00:00:00+00:00",
            "expires_at": "2025-01-01T00:00:10+00:00",
        }
    )

    round_trip_managed_entry = adapter.load_dict(data=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestOpenSearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=OPENSEARCH_VERSIONS_TO_TEST)
    def opensearch_container(self, request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
        version = request.param
        os_image = f"opensearchproject/opensearch:{version}"
        container = DockerContainer(image=os_image)
        container.with_exposed_ports(OPENSEARCH_CONTAINER_PORT)
        container.with_env("discovery.type", "single-node")
        container.with_env("DISABLE_SECURITY_PLUGIN", "true")
        container.with_env("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "TestPassword123!")
        container.waiting_for(LogMessageWaitStrategy("started").with_startup_timeout(120))
        with container:
            yield container

    @pytest.fixture(scope="module")
    def opensearch_url(self, opensearch_container: DockerContainer) -> str:
        host = opensearch_container.get_container_host_ip()
        port = opensearch_container.get_exposed_port(OPENSEARCH_CONTAINER_PORT)
        return f"http://{host}:{port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_opensearch(self, opensearch_container: DockerContainer, opensearch_url: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_opensearch(opensearch_url), tries=WAIT_FOR_OPENSEARCH_TIMEOUT, wait_time=2):
            msg = "OpenSearch failed to start"
            raise OpenSearchFailedToStartError(msg)

    @pytest.fixture
    async def opensearch_client(self, setup_opensearch: None, opensearch_url: str) -> AsyncGenerator[AsyncOpenSearch, None]:
        opensearch_client = get_opensearch_client(opensearch_url)

        async with opensearch_client:
            await cleanup_opensearch_indices(opensearch_client=opensearch_client)

            yield opensearch_client

    @override
    @pytest.fixture
    async def store(self, opensearch_client: AsyncOpenSearch) -> AsyncGenerator[BaseStore, None]:
        store = OpenSearchStore(
            opensearch_client=opensearch_client,
            index_prefix="opensearch-kv-store-e2e-test",
            default_collection="test-collection",
        )

        async with store:
            yield store

    @override
    @pytest.fixture
    async def sanitizing_store(self, opensearch_client: AsyncOpenSearch) -> AsyncGenerator[BaseStore, None]:
        store = OpenSearchStore(
            opensearch_client=opensearch_client,
            index_prefix="opensearch-kv-store-e2e-test",
            default_collection="test-collection",
            key_sanitization_strategy=OpenSearchV1KeySanitizationStrategy(),
            collection_sanitization_strategy=OpenSearchV1CollectionSanitizationStrategy(),
        )

        async with store:
            yield store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: OpenSearchStore, sanitizing_store: OpenSearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_long_key_name(self, store: OpenSearchStore, sanitizing_store: OpenSearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long key name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})

        await sanitizing_store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}

    async def test_put_put_two_indices(self, store: OpenSearchStore, opensearch_client: AsyncOpenSearch):
        await store.put(collection="test_collection", key="test_key", value={"test": "test"})
        await store.put(collection="test_collection_2", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key") == {"test": "test"}
        assert await store.get(collection="test_collection_2", key="test_key") == {"test": "test"}

        indices: dict[str, Any] = await opensearch_client.indices.get(index="opensearch-kv-store-e2e-test-*")
        index_names: list[str] = list(indices.keys())
        assert index_names == snapshot(["opensearch-kv-store-e2e-test-test_collection", "opensearch-kv-store-e2e-test-test_collection_2"])

    async def test_value_stored_as_f_object(self, store: OpenSearchStore, opensearch_client: AsyncOpenSearch):
        """Verify values are stored as f objects, not JSON strings"""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        index_name = store._get_index_name(collection="test")
        doc_id = store._get_document_id(key="test_key")

        response = await opensearch_client.get(index=index_name, id=doc_id)
        assert response["_source"] == snapshot(
            {
                "version": 1,
                "key": "test_key",
                "collection": "test",
                "value": {"flat": {"name": "Alice", "age": 30}},
                "created_at": IsStr(min_length=20, max_length=40),
            }
        )

        # Test with TTL
        await store.put(collection="test", key="test_key", value={"name": "Bob", "age": 25}, ttl=10)
        response = await opensearch_client.get(index=index_name, id=doc_id)
        assert response["_source"] == snapshot(
            {
                "version": 1,
                "key": "test_key",
                "collection": "test",
                "value": {"flat": {"name": "Bob", "age": 25}},
                "created_at": IsStr(min_length=20, max_length=40),
                "expires_at": IsStr(min_length=20, max_length=40),
            }
        )

    @override
    async def test_special_characters_in_collection_name(self, store: OpenSearchStore, sanitizing_store: OpenSearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a special characters in the collection name will not raise an error."""
        await super().test_special_characters_in_collection_name(store=sanitizing_store)
