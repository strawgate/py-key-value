import os
from collections.abc import AsyncGenerator

import pytest
from elasticsearch import Elasticsearch
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.elasticsearch import ElasticsearchStore
from tests.conftest import docker_container
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin, wait_for_store

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB
ES_HOST = "localhost"
ES_PORT = 9200
ES_URL = f"http://{ES_HOST}:{ES_PORT}"
ES_VERSION = "9.1.4"
ES_IMAGE = f"docker.elastic.co/elasticsearch/elasticsearch:{ES_VERSION}"


def get_elasticsearch_client() -> Elasticsearch:
    return Elasticsearch(hosts=[ES_URL])


def ping_elasticsearch() -> bool:
    es_client: Elasticsearch = get_elasticsearch_client()

    return es_client.ping()


class ElasticsearchFailedToStartError(Exception):
    pass


@pytest.mark.skipif(os.getenv("ES_URL") is None, reason="Elasticsearch is not configured")
class TestElasticsearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_elasticsearch(self) -> AsyncGenerator[None, None]:
        with docker_container(
            "elasticsearch-test", ES_IMAGE, {"9200": 9200}, {"discovery.type": "single-node", "xpack.security.enabled": "false"}
        ):
            if not wait_for_store(wait_fn=ping_elasticsearch, max_time=30):
                msg = "Elasticsearch failed to start"
                raise ElasticsearchFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self) -> ElasticsearchStore:
        es_client = get_elasticsearch_client()
        _ = es_client.options(ignore_status=404).indices.delete(index="kv-store-e2e-test")
        return ElasticsearchStore(url=ES_URL, index="kv-store-e2e-test")

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...
