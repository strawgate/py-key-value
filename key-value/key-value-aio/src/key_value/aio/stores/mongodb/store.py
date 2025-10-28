from collections.abc import Sequence
from datetime import datetime
from typing import Any, overload

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS, sanitize_string
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


def document_to_managed_entry(document: dict[str, Any]) -> ManagedEntry:
    """Convert a MongoDB document back to a ManagedEntry.

    This function deserializes a MongoDB document (created by `managed_entry_to_document`) back to a
    ManagedEntry object. It supports both native BSON storage (dict in value.dict field) and legacy
    JSON string storage (string in value.string field) for migration support.

    Args:
        document: The MongoDB document to convert.

    Returns:
        A ManagedEntry object reconstructed from the document.
    """
    # Check if we have the new two-field structure
    value_field = document.get("value")

    if isinstance(value_field, dict):
        # New format: check for dict or string subfields
        if "dict" in value_field:
            # Native storage mode - value is already a dict
            return ManagedEntry.from_dict(
                data={"value": value_field["dict"], **{k: v for k, v in document.items() if k != "value"}}, stringified_value=False
            )
        if "string" in value_field:
            # Legacy storage mode - value is a JSON string
            return ManagedEntry.from_dict(
                data={"value": value_field["string"], **{k: v for k, v in document.items() if k != "value"}}, stringified_value=True
            )

    # Old format: value field is directly a string
    return ManagedEntry.from_dict(data=document, stringified_value=True)


def managed_entry_to_document(key: str, managed_entry: ManagedEntry, *, native_storage: bool = True) -> dict[str, Any]:
    """Convert a ManagedEntry to a MongoDB document for storage.

    This function serializes a ManagedEntry to a MongoDB document format, including the key and all
    metadata (TTL, creation, and expiration timestamps). The value storage format depends on the
    native_storage parameter.

    Args:
        key: The key associated with this entry.
        managed_entry: The ManagedEntry to serialize.
        native_storage: If True (default), store value as native BSON dict in value.dict field.
                       If False, store as JSON string in value.string field for backward compatibility.

    Returns:
        A MongoDB document dict containing the key, value, and all metadata.
    """
    document: dict[str, Any] = {"key": key, "value": {}}

    # Store in appropriate field based on mode
    if native_storage:
        document["value"]["dict"] = managed_entry.value_as_dict
    else:
        document["value"]["string"] = managed_entry.value_as_json

    # Add metadata fields
    if managed_entry.created_at:
        document["created_at"] = managed_entry.created_at
    if managed_entry.expires_at:
        document["expires_at"] = managed_entry.expires_at

    return document


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
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            client: The MongoDB client to use.
            db_name: The name of the MongoDB database.
            coll_name: The name of the MongoDB collection.
            native_storage: Whether to use native BSON storage (True, default) or JSON string storage (False).
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        url: str,
        db_name: str | None = None,
        coll_name: str | None = None,
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            url: The url of the MongoDB cluster.
            db_name: The name of the MongoDB database.
            coll_name: The name of the MongoDB collection.
            native_storage: Whether to use native BSON storage (True, default) or JSON string storage (False).
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        client: AsyncMongoClient[dict[str, Any]] | None = None,
        url: str | None = None,
        db_name: str | None = None,
        coll_name: str | None = None,
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the MongoDB store.

        Args:
            client: The MongoDB client to use (mutually exclusive with url).
            url: The url of the MongoDB cluster (mutually exclusive with client).
            db_name: The name of the MongoDB database.
            coll_name: The name of the MongoDB collection.
            native_storage: Whether to use native BSON storage (True, default) or JSON string storage (False).
                           Native storage stores values as BSON dicts for better query support.
                           Legacy mode stores values as JSON strings for backward compatibility.
            default_collection: The default collection to use if no collection is provided.
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
        """Sanitize a collection name to meet MongoDB naming requirements.

        MongoDB has specific requirements for collection names (length limits, allowed characters).
        This method ensures collection names are compliant by truncating to the maximum allowed length
        and replacing invalid characters with safe alternatives.

        Args:
            collection: The collection name to sanitize.

        Returns:
            A sanitized collection name that meets MongoDB requirements.
        """
        return sanitize_string(value=collection, max_length=MAX_COLLECTION_LENGTH, allowed_characters=ALPHANUMERIC_CHARACTERS)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        # Ensure index on the unique combo key and supporting queries
        collection = self._sanitize_collection_name(collection=collection)

        collection_filter: dict[str, str] = {"name": collection}
        matching_collections: list[str] = await self._db.list_collection_names(filter=collection_filter)

        if matching_collections:
            self._collections_by_name[collection] = self._db[collection]
            return

        new_collection: AsyncCollection[dict[str, Any]] = await self._db.create_collection(name=collection)

        # Index for efficient key lookups
        _ = await new_collection.create_index(keys="key")

        # TTL index for automatic expiration of entries when expires_at is reached
        _ = await new_collection.create_index(keys="expires_at", expireAfterSeconds=0)

        self._collections_by_name[collection] = new_collection

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        sanitized_collection = self._sanitize_collection_name(collection=collection)

        if doc := await self._collections_by_name[sanitized_collection].find_one(filter={"key": key}):
            return ManagedEntry.from_dict(data=doc, stringified_value=True)

        return None

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        sanitized_collection = self._sanitize_collection_name(collection=collection)

        # Use find with $in operator to get multiple documents at once
        cursor = self._collections_by_name[sanitized_collection].find(filter={"key": {"$in": keys}})

        managed_entries_by_key: dict[str, ManagedEntry | None] = dict.fromkeys(keys)

        async for doc in cursor:
            if key := doc.get("key"):
                managed_entries_by_key[key] = document_to_managed_entry(document=doc)

        return [managed_entries_by_key[key] for key in keys]

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        mongo_doc: dict[str, Any] = managed_entry_to_document(key=key, managed_entry=managed_entry, native_storage=self._native_storage)

        sanitized_collection = self._sanitize_collection_name(collection=collection)

        _ = await self._collections_by_name[sanitized_collection].update_one(
            filter={"key": key},
            update={"$set": mongo_doc},
            upsert=True,
        )

    @override
    async def _put_managed_entries(
        self,
        *,
        collection: str,
        keys: Sequence[str],
        managed_entries: Sequence[ManagedEntry],
        ttl: float | None,
        created_at: datetime,
        expires_at: datetime | None,
    ) -> None:
        if not keys:
            return

        sanitized_collection = self._sanitize_collection_name(collection=collection)

        # Use bulk_write for efficient batch operations
        from pymongo import UpdateOne

        operations: list[UpdateOne] = []
        for key, managed_entry in zip(keys, managed_entries, strict=True):
            mongo_doc: dict[str, Any] = managed_entry_to_document(key=key, managed_entry=managed_entry, native_storage=self._native_storage)

            operations.append(
                UpdateOne(
                    filter={"key": key},
                    update={"$set": mongo_doc},
                    upsert=True,
                )
            )

        _ = await self._collections_by_name[sanitized_collection].bulk_write(operations)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        sanitized_collection = self._sanitize_collection_name(collection=collection)

        result: DeleteResult = await self._collections_by_name[sanitized_collection].delete_one(filter={"key": key})
        return bool(result.deleted_count)

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        if not keys:
            return 0

        sanitized_collection = self._sanitize_collection_name(collection=collection)

        # Use delete_many with $in operator for efficient batch deletion
        result: DeleteResult = await self._collections_by_name[sanitized_collection].delete_many(filter={"key": {"$in": keys}})

        return result.deleted_count

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        collections: list[str] = await self._db.list_collection_names(filter={})

        return collections[:limit]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        sanitized_collection = self._sanitize_collection_name(collection=collection)

        _ = await self._db.drop_collection(name_or_collection=sanitized_collection)
        if sanitized_collection in self._collections_by_name:
            del self._collections_by_name[sanitized_collection]
        return True

    @override
    async def _close(self) -> None:
        await self._client.close()
