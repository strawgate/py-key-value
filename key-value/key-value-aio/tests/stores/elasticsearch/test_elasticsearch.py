import os
from collections.abc import AsyncGenerator

import pytest
from elasticsearch import AsyncElasticsearch
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.elasticsearch import ElasticsearchStore
from tests.conftest import docker_container
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB
ES_HOST = "localhost"
ES_PORT = 9200
ES_URL = f"http://{ES_HOST}:{ES_PORT}"

ELASTICSEARCH_VERSIONS_TO_TEST = [
    "9.0.0",  # Minimum supported version
    "9.2.0",  # Latest stable version
]


def get_elasticsearch_client() -> AsyncElasticsearch:
    return AsyncElasticsearch(hosts=[ES_URL])


async def ping_elasticsearch() -> bool:
    es_client: AsyncElasticsearch = get_elasticsearch_client()

    async with es_client:
        return await es_client.ping()


class ElasticsearchFailedToStartError(Exception):
    pass


@pytest.mark.skipif(os.getenv("ES_URL") is None, reason="Elasticsearch is not configured")
class TestElasticsearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=ELASTICSEARCH_VERSIONS_TO_TEST)
    async def setup_elasticsearch(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param
        es_image = f"docker.elastic.co/elasticsearch/elasticsearch:{version}"

        with docker_container(
            "elasticsearch-test", es_image, {"9200": ES_PORT}, {"discovery.type": "single-node", "xpack.security.enabled": "false"}
        ):
            if not await async_wait_for_true(bool_fn=ping_elasticsearch, tries=30, wait_time=2):
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
        es_client = get_elasticsearch_client()
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
