import asyncio
from collections.abc import AsyncGenerator

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.mongodb import MongoStore
from tests.stores.conftest import BaseStoreTests

# MongoDB test configuration
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB = "kvstore_test"

WAIT_FOR_MONGO_TIMEOUT = 30


async def ping_mongo() -> bool:
    client = AsyncIOMotorClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}")
    try:
        await client.admin.command("ping")
    except Exception:
        return False
    else:
        return True
    finally:
        client.close()


async def wait_mongo() -> bool:
    # with a timeout of 30 seconds
    for _ in range(WAIT_FOR_MONGO_TIMEOUT):
        if await ping_mongo():
            return True
        await asyncio.sleep(delay=1)

    return False


class MongoFailedToStartError(Exception):
    pass


class TestMongoStore(BaseStoreTests):
    @pytest.fixture(autouse=True, scope="session")
    async def setup_mongo(self) -> AsyncGenerator[None, None]:
        # Try to connect to existing MongoDB or skip tests if not available
        if not await ping_mongo():
            pytest.skip("MongoDB not available at localhost:27017")

        return

    @override
    @pytest.fixture
    async def store(self, setup_mongo: None) -> MongoStore:  # pyright: ignore[reportUnusedParameter]
        """Create a MongoDB store for testing."""
        # Create the store with test database
        mongo_store = MongoStore(host=MONGO_HOST, port=MONGO_PORT, database=MONGO_DB)

        # Clear the test database
        await mongo_store._database.drop_collection(mongo_store._collection_name)

        return mongo_store

    async def test_mongo_connection_string(self) -> None:
        """Test MongoDB store creation with connection string."""
        connection_string = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"
        store = MongoStore(connection_string=connection_string)

        await store._database.drop_collection(store._collection_name)
        await store.put(collection="test", key="conn_test", value={"test": "value"})
        result = await store.get(collection="test", key="conn_test")
        assert result == {"test": "value"}

    async def test_mongo_client_connection(self) -> None:
        """Test MongoDB store creation with existing client."""
        client = AsyncIOMotorClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}")
        store = MongoStore(client=client, database=MONGO_DB)

        await store._database.drop_collection(store._collection_name)
        await store.put(collection="test", key="client_test", value={"test": "value"})
        result = await store.get(collection="test", key="client_test")
        assert result == {"test": "value"}

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseKVStore): ...
