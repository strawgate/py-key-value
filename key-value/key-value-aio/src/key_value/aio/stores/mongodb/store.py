from datetime import datetime
from typing import Any, TypedDict, overload

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS, sanitize_string
from key_value.shared.utils.time_to_live import now
from typing_extensions import Self, override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyCollectionStore, BaseEnumerateCollectionsStore, BaseStore

try:
    from pymongo import AsyncMongoClient
    from pymongo.asynchronous.collection import AsyncCollection
    from pymongo.asynchronous.database import AsyncDatabase
    from pymongo.results import DeleteResult  # noqa: TC002
except ImportError as e:
    msg = "MongoDBStore requires py-key-value-aio[mongodb]"
    raise ImportError(msg) from e


DEFAULT_DB = "kv-store-adapter"
DEFAULT_COLLECTION = "kv"

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

# MongoDB collection name length limit
# https://www.mongodb.com/docs/manual/reference/limits/
# For unsharded collections and views, the namespace length limit is 255 bytes.
# For sharded collections, the namespace length limit is 235 bytes.
# So limit the collection name to 200 bytes
MAX_COLLECTION_LENGTH = 200
COLLECTION_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_"


class MongoDBStoreDocument(TypedDict):
    value: dict[str, Any]

    created_at: datetime | None
    expires_at: datetime | None


class MongoDBStore(BaseEnumerateCollectionsStore, BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """MongoDB-based key-value store using Motor (async MongoDB driver)."""

    _client: AsyncMongoClient[dict[str, Any]]
    _db: AsyncDatabase[dict[str, Any]]
    _collections_by_name: dict[str, AsyncCollection[dict[str, Any]]]
    _native_storage: bool

    @overload
    def __init__(
        self,
        *,
        client: AsyncMongoClient[dict[str, Any]],
        db_name: str | None = None,
        coll_name: str | None = None,
        default_collection: str | None = None,
        native_storage: bool = False,
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            client: The MongoDB client to use.
            db_name: The name of the MongoDB database.
            coll_name: The name of the MongoDB collection.
            default_collection: The default collection to use if no collection is provided.
            native_storage: If True, store values as native BSON documents. If False (default),
                store values as JSON strings. WARNING: Switching between modes requires data migration.
        """

    @overload
    def __init__(
        self, *, url: str, db_name: str | None = None, coll_name: str | None = None, default_collection: str | None = None, native_storage: bool = False
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            url: The url of the MongoDB cluster.
            db_name: The name of the MongoDB database.
            coll_name: The name of the MongoDB collection.
            default_collection: The default collection to use if no collection is provided.
            native_storage: If True, store values as native BSON documents. If False (default),
                store values as JSON strings. WARNING: Switching between modes requires data migration.
        """

    def __init__(
        self,
        *,
        client: AsyncMongoClient[dict[str, Any]] | None = None,
        url: str | None = None,
        db_name: str | None = None,
        coll_name: str | None = None,
        default_collection: str | None = None,
        native_storage: bool = False,
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            native_storage: If True, store values as native BSON documents with datetime objects.
                If False (default), store values as JSON strings with ISO date strings.
                WARNING: Switching between modes is a breaking change that requires data migration.
        """

        if client:
            self._client = client
        elif url:
            self._client = AsyncMongoClient(url)
        else:
            # Defaults to localhost
            self._client = AsyncMongoClient()

        db_name = db_name or DEFAULT_DB
        coll_name = coll_name or DEFAULT_COLLECTION

        self._db = self._client[db_name]
        self._collections_by_name = {}
        self._native_storage = native_storage

        super().__init__(default_collection=default_collection)

    @override
    async def __aenter__(self) -> Self:
        _ = await self._client.__aenter__()
        await super().__aenter__()
        return self

    @override
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # pyright: ignore[reportAny]
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    def _sanitize_collection_name(self, collection: str) -> str:
        return sanitize_string(value=collection, max_length=MAX_COLLECTION_LENGTH, allowed_characters=ALPHANUMERIC_CHARACTERS)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        # Ensure index on the unique combo key and supporting queries
        collection = self._sanitize_collection_name(collection=collection)

        collection_filter: dict[str, str] = {"name": collection}
        matching_collections: list[str] = await self._db.list_collection_names(filter=collection_filter)

        if matching_collections:
            self._collections_by_name[collection] = self._db[collection]
            # Validate indexes for existing collection
            await self._validate_collection_indexes(collection=collection)
            return

        new_collection: AsyncCollection[dict[str, Any]] = await self._db.create_collection(name=collection)

        _ = await new_collection.create_index(keys="key")

        # Create TTL index for automatic expiration (only when using native storage)
        if self._native_storage:
            _ = await new_collection.create_index(keys="expires_at", expireAfterSeconds=0)

        self._collections_by_name[collection] = new_collection

    async def _validate_collection_indexes(self, *, collection: str) -> None:
        """Validate that the collection indexes match the configured storage mode."""
        try:
            coll = self._collections_by_name[collection]
            indexes = await coll.list_indexes().to_list(length=None)

            # Check for TTL index on expires_at
            has_ttl_index = any(
                idx.get("key", {}).get("expires_at") is not None and idx.get("expireAfterSeconds") is not None for idx in indexes
            )

            if self._native_storage and not has_ttl_index:
                msg = (
                    f"Collection '{collection}' is missing TTL index on 'expires_at' field, "
                    f"but store is configured for native_storage mode. "
                    f"To fix this, either: 1) Recreate the collection with native_storage=True, "
                    f"or 2) Manually create the TTL index: db.{collection}.createIndex({{expires_at: 1}}, {{expireAfterSeconds: 0}})"
                )
                raise ValueError(msg)
        except ValueError:
            # Re-raise our validation errors
            raise
        except Exception:
            # Suppress other errors (e.g., connection issues) to allow store to work
            pass

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        collection = self._sanitize_collection_name(collection=collection)

        doc: dict[str, Any] | None = await self._collections_by_name[collection].find_one(filter={"key": key})

        if not doc:
            return None

        if self._native_storage:
            # Native storage mode: Get value as BSON document
            value: dict[str, Any] | None = doc.get("value")

            if not isinstance(value, dict):
                return None

            # Parse datetime objects directly and validate types
            created_at: datetime | None = doc.get("created_at")
            expires_at: datetime | None = doc.get("expires_at")

            # Validate datetime types to detect storage mode mismatches
            if created_at is not None and not isinstance(created_at, datetime):
                msg = (
                    f"Data for key '{key}' has invalid created_at type: expected datetime but got {type(created_at).__name__}. "
                    f"This may indicate a storage mode mismatch."
                )
                raise TypeError(msg)
            if expires_at is not None and not isinstance(expires_at, datetime):
                msg = (
                    f"Data for key '{key}' has invalid expires_at type: expected datetime but got {type(expires_at).__name__}. "
                    f"This may indicate a storage mode mismatch."
                )
                raise TypeError(msg)

            return ManagedEntry(
                value=value,
                created_at=created_at,
                expires_at=expires_at,
            )
        else:
            # JSON string mode: Get value as JSON string and parse it
            json_value: str | None = doc.get("value")

            if not isinstance(json_value, str):
                return None

            return ManagedEntry.from_json(json_str=json_value)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        collection = self._sanitize_collection_name(collection=collection)

        if self._native_storage:
            # Native storage mode: Store value as BSON document
            set_fields: dict[str, Any] = {
                "value": managed_entry.value,  # Store as BSON document
                "updated_at": now(),
            }

            set_on_insert_fields: dict[str, Any] = {}

            # Store as datetime objects (use $setOnInsert for immutable fields)
            if managed_entry.created_at:
                set_on_insert_fields["created_at"] = managed_entry.created_at
            if managed_entry.expires_at:
                # expires_at can change, so use $set
                set_fields["expires_at"] = managed_entry.expires_at

            update_doc: dict[str, Any] = {"$set": set_fields}
            if set_on_insert_fields:
                update_doc["$setOnInsert"] = set_on_insert_fields

            _ = await self._collections_by_name[collection].update_one(
                filter={"key": key},
                update=update_doc,
                upsert=True,
            )
        else:
            # JSON string mode: Store value as JSON string
            set_fields: dict[str, Any] = {
                "value": managed_entry.to_json(),  # Store as JSON string
                "updated_at": now(),
            }

            _ = await self._collections_by_name[collection].update_one(
                filter={"key": key},
                update={"$set": set_fields},
                upsert=True,
            )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        collection = self._sanitize_collection_name(collection=collection)

        result: DeleteResult = await self._collections_by_name[collection].delete_one(filter={"key": key})
        return bool(result.deleted_count)

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        collections: list[str] = await self._db.list_collection_names(filter={})

        return collections[:limit]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        collection = self._sanitize_collection_name(collection=collection)

        _ = await self._db.drop_collection(name_or_collection=collection)
        if collection in self._collections_by_name:
            del self._collections_by_name[collection]
        return True

    @override
    async def _close(self) -> None:
        await self._client.close()
