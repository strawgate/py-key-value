import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from pymongo import AsyncMongoClient
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.mongodb import MongoDBStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# MongoDB test configuration
MONGODB_HOST = "localhost"
MONGODB_HOST_PORT = 27017
MONGODB_TEST_DB = "kv-store-adapter-tests"

WAIT_FOR_MONGODB_TIMEOUT = 30

MONGODB_VERSIONS_TO_TEST = [
    "5.0",  # Older supported version
    "8.0",  # Latest stable version
]


async def ping_mongodb() -> bool:
    try:
        client: AsyncMongoClient[Any] = AsyncMongoClient[Any](host=MONGODB_HOST, port=MONGODB_HOST_PORT)
        _ = await client.list_database_names()
    except Exception:
        return False

    return True


class MongoDBFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestMongoDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session", params=MONGODB_VERSIONS_TO_TEST)
    async def setup_mongodb(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container("mongodb-test", f"mongo:{version}", {str(MONGODB_HOST_PORT): MONGODB_HOST_PORT}):
            if not await async_wait_for_true(bool_fn=ping_mongodb, tries=30, wait_time=1):
                msg = f"MongoDB {version} failed to start"
                raise MongoDBFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_mongodb: None) -> MongoDBStore:
        store = MongoDBStore(url=f"mongodb://{MONGODB_HOST}:{MONGODB_HOST_PORT}", db_name=MONGODB_TEST_DB)
        # Ensure a clean db by dropping our default test collection if it exists
        with contextlib.suppress(Exception):
            _ = await store._client.drop_database(name_or_database=MONGODB_TEST_DB)  # pyright: ignore[reportPrivateUsage]

        return store

    @pytest.fixture
    async def mongodb_store(self, store: MongoDBStore) -> MongoDBStore:
        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_mongodb_collection_name_sanitization(self, mongodb_store: MongoDBStore):
        """Tests that a special characters in the collection name will not raise an error."""
        await mongodb_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await mongodb_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

        collections = await mongodb_store.collections()
        assert collections == snapshot(["test_collection_-daf4a2ec"])
