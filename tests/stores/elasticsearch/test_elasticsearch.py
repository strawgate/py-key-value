import os
from collections.abc import AsyncGenerator

import pytest
from elasticsearch import AsyncElasticsearch
from typing_extensions import override

from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.stores.elasticsearch import ElasticsearchStore
from tests.stores.conftest import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 1 * 1024 * 1024  # 1MB


@pytest.fixture
async def elasticsearch_client() -> AsyncGenerator[AsyncElasticsearch, None]:
    es_url = os.getenv("ES_URL")
    es_api_key = os.getenv("ES_API_KEY")

    assert isinstance(es_url, str)

    assert isinstance(es_api_key, str)

    client = AsyncElasticsearch(hosts=[es_url], api_key=es_api_key)

    async with client:
        yield client


@pytest.mark.skipif(os.getenv("ES_URL") is None, reason="Elasticsearch is not configured")
class TestElasticsearchStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, elasticsearch_client: AsyncElasticsearch) -> ElasticsearchStore:
        _ = await elasticsearch_client.options(ignore_status=404).indices.delete(index="kv-store-e2e-test")
        return ElasticsearchStore(elasticsearch_client=elasticsearch_client, index="kv-store-e2e-test")

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.mark.skip(reason="Skip concurrent tests on distributed caches")
    @override
    async def test_concurrent_operations(self, store: BaseStore): ...
