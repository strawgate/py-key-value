from typing import overload
from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


class MongoStore(BaseManagedKVStore):
    """MongoDB-based key-value store."""

    _client: AsyncIOMotorClient
    _database: AsyncIOMotorDatabase
    _collection_name: str

    @overload
    def __init__(self, *, client: AsyncIOMotorClient, database: str = "kvstore", collection: str = "entries") -> None: ...

    @overload
    def __init__(self, *, connection_string: str, database: str = "kvstore", collection: str = "entries") -> None: ...

    @overload
    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 27017,
        username: str | None = None,
        password: str | None = None,
        database: str = "kvstore",
        collection: str = "entries",
    ) -> None: ...

    def __init__(
        self,
        *,
        client: AsyncIOMotorClient | None = None,
        connection_string: str | None = None,
        host: str = "localhost",
        port: int = 27017,
        username: str | None = None,
        password: str | None = None,
        database: str = "kvstore",
        collection: str = "entries",
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            client: An existing AsyncIOMotorClient to use.
            connection_string: MongoDB connection string (e.g., mongodb://localhost:27017/kvstore).
            host: MongoDB host. Defaults to localhost.
            port: MongoDB port. Defaults to 27017.
            username: MongoDB username. Defaults to None.
            password: MongoDB password. Defaults to None.
            database: Database name to use. Defaults to kvstore.
            collection: Collection name to use. Defaults to entries.
        """
        if client:
            self._client = client
        elif connection_string:
            self._client = AsyncIOMotorClient(connection_string)
            # Extract database name from connection string if not in path
            parsed = urlparse(connection_string)
            if parsed.path and parsed.path != "/" and database != "kvstore":
                pass  # Keep provided database name
            elif parsed.path and parsed.path != "/":
                database = parsed.path.lstrip("/")
        else:
            # Build connection string from individual parameters
            auth_str = f"{username}:{password}@" if username and password else ""
            connection_str = f"mongodb://{auth_str}{host}:{port}"
            self._client = AsyncIOMotorClient(connection_str)

        self._database = self._client[database]
        self._collection_name = collection
        super().__init__()

    @property
    def _collection(self) -> AsyncIOMotorCollection:
        """Get the collection for storing entries."""
        return self._database[self._collection_name]

    @override
    async def setup(self) -> None:
        """Initialize the MongoDB store by testing connectivity and creating indexes."""
        try:
            # Test connection
            await self._client.admin.command("ping")

            # Create compound index on collection+key for efficient lookups
            await self._collection.create_index([("collection", 1), ("key", 1)], unique=True)

            # Create TTL index for automatic expiration
            await self._collection.create_index("expires_at", expireAfterSeconds=0)
        except Exception as e:
            raise StoreConnectionError(message=f"Failed to connect to MongoDB: {e}") from e

    @override
    async def setup_collection(self, collection: str) -> None:
        """Setup collection-specific resources (no-op for MongoDB)."""
        # MongoDB collections are created automatically when first document is inserted

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        doc = await self._collection.find_one({"collection": collection, "key": key})

        if doc is None:
            return None

        # Convert MongoDB document to ManagedEntry
        return ManagedEntry(
            collection=doc["collection"],
            key=doc["key"],
            value=doc["value"],
            created_at=doc.get("created_at"),
            ttl=doc.get("ttl"),
            expires_at=doc.get("expires_at"),
        )

    @override
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None,
    ) -> None:
        doc = {
            "collection": collection,
            "key": key,
            "value": cache_entry.value,
            "created_at": cache_entry.created_at,
            "ttl": cache_entry.ttl,
            "expires_at": cache_entry.expires_at,
        }

        # Use upsert to replace existing entries
        await self._collection.replace_one(
            {"collection": collection, "key": key},
            doc,
            upsert=True,
        )

    @override
    async def delete(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        result = await self._collection.delete_one({"collection": collection, "key": key})
        return result.deleted_count > 0

    @override
    async def keys(self, collection: str) -> list[str]:
        await self.setup_collection_once(collection=collection)

        cursor = self._collection.find({"collection": collection}, {"key": 1})
        return [doc["key"] async for doc in cursor]

    @override
    async def clear_collection(self, collection: str) -> int:
        await self.setup_collection_once(collection=collection)

        result = await self._collection.delete_many({"collection": collection})
        return result.deleted_count

    @override
    async def list_collections(self) -> list[str]:
        await self.setup_once()

        pipeline = [
            {"$group": {"_id": "$collection"}},
            {"$project": {"collection": "$_id", "_id": 0}},
        ]

        cursor = self._collection.aggregate(pipeline)
        return [doc["collection"] async for doc in cursor]

    @override
    async def cull(self) -> None:
        """MongoDB handles TTL automatically, so this is a no-op."""
        # MongoDB's TTL indexes handle expiration automatically
