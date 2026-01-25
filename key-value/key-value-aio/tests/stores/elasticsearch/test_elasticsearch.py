from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest
from dirty_equals import IsFloat, IsStr
from elasticsearch import AsyncElasticsearch
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.managed_entry import ManagedEntry
from testcontainers.elasticsearch import ElasticSearchContainer
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.elasticsearch import ElasticsearchStore
from key_value.aio.stores.elasticsearch.store import (
    ElasticsearchSerializationAdapter,
    ElasticsearchV1CollectionSanitizationStrategy,
    ElasticsearchV1KeySanitizationStrategy,
)
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

if TYPE_CHECKING:
    from elastic_transport._response import ObjectApiResponse

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB

WAIT_FOR_ELASTICSEARCH_TIMEOUT = 30

ELASTICSEARCH_VERSIONS_TO_TEST = [
    "9.0.0",  # Released Apr 2025
    "9.2.0",  # Released Oct 2025
]


async def ping_elasticsearch(es_url: str) -> bool:
    es_client: AsyncElasticsearch = AsyncElasticsearch(hosts=[es_url])

    async with es_client:
        if not await es_client.ping():
            return False

        status: ObjectApiResponse[dict[str, Any]] = await es_client.options(ignore_status=404).cluster.health(wait_for_status="green")

        return status.body.get("status") == "green"


async def cleanup_elasticsearch_indices(elasticsearch_client: AsyncElasticsearch):
    indices = await elasticsearch_client.options(ignore_status=404).indices.get(index="kv-store-e2e-test-*")
    for index in indices:
        _ = await elasticsearch_client.options(ignore_status=404).indices.delete(index=index)


class ElasticsearchFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    adapter = ElasticsearchSerializationAdapter()
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "version": 1,
            "value": {"flattened": {"test": "test"}},
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
class TestElasticsearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=ELASTICSEARCH_VERSIONS_TO_TEST)
    def elasticsearch_container(self, request: pytest.FixtureRequest):
        version = request.param
        es_image = f"docker.elastic.co/elasticsearch/elasticsearch:{version}"
        container = ElasticSearchContainer(image=es_image, mem_limit="2g")
        # Configure single-node discovery and disable security
        container.with_env("discovery.type", "single-node")
        container.with_env("xpack.security.enabled", "false")
        container.start()
        yield container
        container.stop()

    @pytest.fixture(scope="session")
    def es_url(self, elasticsearch_container: ElasticSearchContainer) -> str:
        host = elasticsearch_container.get_container_host_ip()
        port = elasticsearch_container.get_exposed_port(9200)
        return f"http://{host}:{port}"

    @pytest.fixture(scope="session")
    async def setup_elasticsearch(self, elasticsearch_container: ElasticSearchContainer, es_url: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_elasticsearch(es_url), tries=WAIT_FOR_ELASTICSEARCH_TIMEOUT, wait_time=2):
            msg = "Elasticsearch failed to start"
            raise ElasticsearchFailedToStartError(msg)

    @pytest.fixture
    async def es_client(self, setup_elasticsearch: None, es_url: str) -> AsyncGenerator[AsyncElasticsearch, None]:
        async with AsyncElasticsearch(hosts=[es_url]) as es_client:
            yield es_client

    @override
    @pytest.fixture
    async def store(self, setup_elasticsearch: None, es_url: str) -> ElasticsearchStore:
        return ElasticsearchStore(url=es_url, index_prefix="kv-store-e2e-test")

    @pytest.fixture
    async def sanitizing_store(self, setup_elasticsearch: None, es_url: str) -> ElasticsearchStore:
        return ElasticsearchStore(
            url=es_url,
            index_prefix="kv-store-e2e-test",
            key_sanitization_strategy=ElasticsearchV1KeySanitizationStrategy(),
            collection_sanitization_strategy=ElasticsearchV1CollectionSanitizationStrategy(),
        )

    @pytest.fixture(autouse=True)
    async def cleanup_elasticsearch_indices(self, es_client: AsyncElasticsearch):
        await cleanup_elasticsearch_indices(elasticsearch_client=es_client)
        yield
        await cleanup_elasticsearch_indices(elasticsearch_client=es_client)

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: ElasticsearchStore, sanitizing_store: ElasticsearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_long_key_name(self, store: ElasticsearchStore, sanitizing_store: ElasticsearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a long key name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})

        await sanitizing_store.put(collection="test_collection", key="test_key" * 100, value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection", key="test_key" * 100) == {"test": "test"}

    async def test_put_put_two_indices(self, store: ElasticsearchStore, es_client: AsyncElasticsearch):
        await store.put(collection="test_collection", key="test_key", value={"test": "test"})
        await store.put(collection="test_collection_2", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key") == {"test": "test"}
        assert await store.get(collection="test_collection_2", key="test_key") == {"test": "test"}

        indices = await es_client.options(ignore_status=404).indices.get(index="kv-store-e2e-test-*")
        assert len(indices.body) == 2
        index_names: list[str] = [str(key) for key in indices]
        assert index_names == snapshot(["kv-store-e2e-test-test_collection", "kv-store-e2e-test-test_collection_2"])

    async def test_value_stored_as_flattened_object(self, store: ElasticsearchStore, es_client: AsyncElasticsearch):
        """Verify values are stored as flattened objects, not JSON strings"""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Check raw Elasticsearch document using public sanitization methods
        # Note: We need to access these internal methods for testing the storage format
        index_name = store._get_index_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        doc_id = store._get_document_id(key="test_key")  # pyright: ignore[reportPrivateUsage]

        response = await es_client.get(index=index_name, id=doc_id)
        assert response.body["_source"] == snapshot(
            {
                "version": 1,
                "key": "test_key",
                "collection": "test",
                "value": {"flattened": {"name": "Alice", "age": 30}},
                "created_at": IsStr(min_length=20, max_length=40),
            }
        )

        # Test with TTL
        await store.put(collection="test", key="test_key", value={"name": "Bob", "age": 25}, ttl=10)
        response = await es_client.get(index=index_name, id=doc_id)
        assert response.body["_source"] == snapshot(
            {
                "version": 1,
                "key": "test_key",
                "collection": "test",
                "value": {"flattened": {"name": "Bob", "age": 25}},
                "created_at": IsStr(min_length=20, max_length=40),
                "expires_at": IsStr(min_length=20, max_length=40),
            }
        )

    @override
    async def test_special_characters_in_collection_name(self, store: ElasticsearchStore, sanitizing_store: ElasticsearchStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that a special characters in the collection name will not raise an error."""
        await super().test_special_characters_in_collection_name(store=sanitizing_store)
