from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from dirty_equals import IsFloat
from elasticsearch import AsyncElasticsearch
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.elasticsearch import ElasticsearchStore
from key_value.aio.stores.elasticsearch.store import managed_entry_to_document, source_to_managed_entry
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB
ES_HOST = "localhost"
ES_PORT = 9200
ES_URL = f"http://{ES_HOST}:{ES_PORT}"
ES_CONTAINER_PORT = 9200

WAIT_FOR_ELASTICSEARCH_TIMEOUT = 30

ELASTICSEARCH_VERSIONS_TO_TEST = [
    "9.0.0",  # Released Apr 2025
    "9.2.0",  # Released Oct 2025
]


def get_elasticsearch_client() -> AsyncElasticsearch:
    return AsyncElasticsearch(hosts=[ES_URL])


async def ping_elasticsearch() -> bool:
    es_client: AsyncElasticsearch = get_elasticsearch_client()

    async with es_client:
        return await es_client.ping()


class ElasticsearchFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    document = managed_entry_to_document(collection="test_collection", key="test_key", managed_entry=managed_entry)

    assert document == snapshot(
        {
            "collection": "test_collection",
            "key": "test_key",
            "value": '{"test": "test"}',
            "created_at": "2025-01-01T00:00:00+00:00",
            "expires_at": "2025-01-01T00:00:10+00:00",
        }
    )

    round_trip_managed_entry = source_to_managed_entry(source=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


def test_managed_entry_document_conversion_native_storage():
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    document = managed_entry_to_document(collection="test_collection", key="test_key", managed_entry=managed_entry, native_storage=True)

    assert document == snapshot(
        {
            "collection": "test_collection",
            "key": "test_key",
            "value_flattened": {"test": "test"},
            "created_at": "2025-01-01T00:00:00+00:00",
            "expires_at": "2025-01-01T00:00:10+00:00",
        }
    )

    round_trip_managed_entry = source_to_managed_entry(source=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestElasticsearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=ELASTICSEARCH_VERSIONS_TO_TEST)
    async def setup_elasticsearch(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param
        es_image = f"docker.elastic.co/elasticsearch/elasticsearch:{version}"

        with docker_container(
            f"elasticsearch-test-{version}",
            es_image,
            {str(ES_CONTAINER_PORT): ES_PORT},
            {"discovery.type": "single-node", "xpack.security.enabled": "false"},
        ):
            if not await async_wait_for_true(bool_fn=ping_elasticsearch, tries=WAIT_FOR_ELASTICSEARCH_TIMEOUT, wait_time=2):
                msg = f"Elasticsearch {version} failed to start"
                raise ElasticsearchFailedToStartError(msg)

            yield

    @pytest.fixture
    async def es_client(self) -> AsyncGenerator[AsyncElasticsearch, None]:
        async with AsyncElasticsearch(hosts=[ES_URL]) as es_client:
            yield es_client

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ElasticsearchStore, None]:
        async with get_elasticsearch_client() as es_client:
            indices = await es_client.options(ignore_status=404).indices.get(index="kv-store-e2e-test-*")
            for index in indices:
                _ = await es_client.options(ignore_status=404).indices.delete(index=index)
        async with ElasticsearchStore(url=ES_URL, index_prefix="kv-store-e2e-test") as store:
            yield store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...

    async def test_put_put_two_indices(self, store: ElasticsearchStore, es_client: AsyncElasticsearch):
        await store.put(collection="test_collection", key="test_key", value={"test": "test"})
        await store.put(collection="test_collection_2", key="test_key", value={"test": "test"})
        assert await store.get(collection="test_collection", key="test_key") == {"test": "test"}
        assert await store.get(collection="test_collection_2", key="test_key") == {"test": "test"}

        indices = await es_client.options(ignore_status=404).indices.get(index="kv-store-e2e-test-*")
        assert len(indices.body) == 2
        assert "kv-store-e2e-test-test_collection" in indices
        assert "kv-store-e2e-test-test_collection_2" in indices


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestElasticsearchStoreNativeMode(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test ElasticsearchStore with native_storage=True"""

    @pytest.fixture(autouse=True, scope="session", params=ELASTICSEARCH_VERSIONS_TO_TEST)
    async def setup_elasticsearch(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param
        es_image = f"docker.elastic.co/elasticsearch/elasticsearch:{version}"

        with docker_container(
            f"elasticsearch-test-native-{version}",
            es_image,
            {str(ES_CONTAINER_PORT): ES_PORT},
            {"discovery.type": "single-node", "xpack.security.enabled": "false"},
        ):
            if not await async_wait_for_true(bool_fn=ping_elasticsearch, tries=WAIT_FOR_ELASTICSEARCH_TIMEOUT, wait_time=2):
                msg = f"Elasticsearch {version} failed to start"
                raise ElasticsearchFailedToStartError(msg)

            yield

    @pytest.fixture
    async def es_client(self) -> AsyncGenerator[AsyncElasticsearch, None]:
        async with AsyncElasticsearch(hosts=[ES_URL]) as es_client:
            yield es_client

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ElasticsearchStore, None]:
        async with get_elasticsearch_client() as es_client:
            indices = await es_client.options(ignore_status=404).indices.get(index="kv-store-native-test-*")
            for index in indices:
                _ = await es_client.options(ignore_status=404).indices.delete(index=index)
        async with ElasticsearchStore(url=ES_URL, index_prefix="kv-store-native-test", native_storage=True) as store:
            yield store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...

    async def test_value_stored_as_flattened_object(self, store: ElasticsearchStore, es_client: AsyncElasticsearch):
        """Verify values are stored as flattened objects, not JSON strings"""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Check raw Elasticsearch document using public sanitization methods
        # Note: We need to access these internal methods for testing the storage format
        index_name = store._sanitize_index_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        doc_id = store._sanitize_document_id(key="test_key")  # pyright: ignore[reportPrivateUsage]

        response = await es_client.get(index=index_name, id=doc_id)
        source = response.body["_source"]

        # Should have value_flattened field as dict
        assert "value_flattened" in source
        assert isinstance(source["value_flattened"], dict)
        assert source["value_flattened"] == {"name": "Alice", "age": 30}

        # Should NOT have value field (JSON string)
        assert "value" not in source or source.get("value") is None

    async def test_migration_from_legacy_mode(self, store: ElasticsearchStore, es_client: AsyncElasticsearch):
        """Verify native mode can read legacy JSON string data"""
        # Manually insert a legacy document with JSON string value
        index_name = store._sanitize_index_name(collection="test")  # pyright: ignore[reportPrivateUsage]
        doc_id = store._sanitize_document_id(key="legacy_key")  # pyright: ignore[reportPrivateUsage]

        await es_client.index(
            index=index_name,
            id=doc_id,
            body={
                "collection": "test",
                "key": "legacy_key",
                "value": '{"legacy": "data"}',  # JSON string
            },
        )
        await es_client.indices.refresh(index=index_name)

        # Should be able to read it in native mode
        result = await store.get(collection="test", key="legacy_key")
        assert result == {"legacy": "data"}
