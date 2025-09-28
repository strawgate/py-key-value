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
ES_VERSION = "9.1.4"
ES_IMAGE = f"docker.elastic.co/elasticsearch/elasticsearch:{ES_VERSION}"


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
    @pytest.fixture(autouse=True, scope="session")
    async def setup_elasticsearch(self) -> AsyncGenerator[None, None]:
        with docker_container(
            "elasticsearch-test", ES_IMAGE, {"9200": 9200}, {"discovery.type": "single-node", "xpack.security.enabled": "false"}
        ):
            if not await async_wait_for_true(bool_fn=ping_elasticsearch, tries=30, wait_time=1):
                msg = "Elasticsearch failed to start"
                raise ElasticsearchFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ElasticsearchStore, None]:
        es_client = get_elasticsearch_client()
        _ = await es_client.options(ignore_status=404).indices.delete(index="kv-store-e2e-test")
        async with ElasticsearchStore(url=ES_URL, index="kv-store-e2e-test") as store:
            yield store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...
