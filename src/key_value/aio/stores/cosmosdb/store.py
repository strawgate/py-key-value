from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, overload

from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyCollectionStore, BaseStore
from key_value.shared.managed_entry import ManagedEntry
from key_value.shared.sanitization import HybridSanitizationStrategy, SanitizationStrategy
from key_value.shared.sanitize import ALPHANUMERIC_CHARACTERS

try:
    from azure.cosmos import PartitionKey, exceptions
    from azure.cosmos.aio import ContainerProxy, CosmosClient, DatabaseProxy
except ImportError as e:
    msg = "CosmosDBStore requires py-key-value-aio[cosmosdb]"
    raise ImportError(msg) from e


DEFAULT_DATABASE = "kv-store-adapter"
DEFAULT_CONTAINER = "kv"

# Azure Cosmos DB container name limits
# https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits
# Container names must be between 1-256 characters
MAX_COLLECTION_LENGTH = 200
COLLECTION_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_-"


def _create_cosmosdb_client(*, url: str, credential: str) -> CosmosClient:
    """Create a CosmosClient instance.

    Args:
        url: The Cosmos DB account URL.
        credential: The Cosmos DB account key or credential.

    Returns:
        A CosmosClient instance.
    """
    return CosmosClient(url=url, credential=credential)


async def _create_database_if_not_exists(client: CosmosClient, database_name: str) -> DatabaseProxy:
    """Create a database if it doesn't exist.

    Args:
        client: The CosmosClient instance.
        database_name: The name of the database to create.

    Returns:
        The DatabaseProxy for the database.
    """
    return await client.create_database_if_not_exists(id=database_name)


async def _create_container_if_not_exists(
    database: DatabaseProxy,
    container_name: str,
    *,
    partition_key_path: str = "/collection",
    default_ttl: int | None = None,
) -> ContainerProxy:
    """Create a container if it doesn't exist.

    Args:
        database: The DatabaseProxy instance.
        container_name: The name of the container to create.
        partition_key_path: The path for the partition key. Defaults to "/collection".
        default_ttl: Default TTL for items in seconds. None means no TTL.

    Returns:
        The ContainerProxy for the container.
    """
    kwargs: dict[str, Any] = {
        "id": container_name,
        "partition_key": PartitionKey(path=partition_key_path),
    }

    # Only set default_ttl if explicitly provided
    # -1 means items don't expire by default but TTL can be set per-item
    if default_ttl is not None:
        kwargs["default_ttl"] = default_ttl

    container: ContainerProxy = await database.create_container_if_not_exists(**kwargs)  # pyright: ignore[reportUnknownVariableType]
    return container  # pyright: ignore[reportUnknownVariableType]


async def _upsert_item(container: ContainerProxy, item: dict[str, Any]) -> None:
    """Upsert an item into a container.

    Args:
        container: The ContainerProxy instance.
        item: The item to upsert.
    """
    _ = await container.upsert_item(body=item)


async def _read_item(container: ContainerProxy, item_id: str, partition_key: str) -> dict[str, Any] | None:
    """Read an item from a container.

    Args:
        container: The ContainerProxy instance.
        item_id: The ID of the item to read.
        partition_key: The partition key value.

    Returns:
        The item if found, None otherwise.
    """
    try:
        return await container.read_item(item=item_id, partition_key=partition_key)
    except exceptions.CosmosResourceNotFoundError:
        return None


async def _delete_item(container: ContainerProxy, item_id: str, partition_key: str) -> bool:
    """Delete an item from a container.

    Args:
        container: The ContainerProxy instance.
        item_id: The ID of the item to delete.
        partition_key: The partition key value.

    Returns:
        True if the item was deleted, False if it didn't exist.
    """
    try:
        await container.delete_item(item=item_id, partition_key=partition_key)
    except exceptions.CosmosResourceNotFoundError:
        return False
    else:
        return True


async def _query_items(
    container: ContainerProxy,
    query: str,
    parameters: list[dict[str, Any]] | None = None,
    partition_key: str | None = None,
) -> list[dict[str, Any]]:
    """Query items from a container.

    Args:
        container: The ContainerProxy instance.
        query: The SQL query string.
        parameters: Query parameters.
        partition_key: Optional partition key to scope the query.

    Returns:
        List of matching items.
    """
    items: list[dict[str, Any]] = []
    query_kwargs: dict[str, Any] = {"query": query}
    if parameters:
        query_kwargs["parameters"] = parameters
    if partition_key:
        query_kwargs["partition_key"] = partition_key

    async for item in container.query_items(**query_kwargs):
        items.append(item)  # noqa: PERF401 - async comprehensions not supported here
    return items


class CosmosDBV1CollectionSanitizationStrategy(HybridSanitizationStrategy):
    """Sanitization strategy for Cosmos DB collection names."""

    def __init__(self) -> None:
        super().__init__(
            replacement_character="_",
            max_length=MAX_COLLECTION_LENGTH,
            allowed_characters=COLLECTION_ALLOWED_CHARACTERS,
        )


class CosmosDBStore(BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """Azure Cosmos DB-based key-value store.

    This store uses Azure Cosmos DB's NoSQL API with the following structure:
    - One database per store instance
    - One container for all collections (using partition key)
    - Collections are represented as partition key values
    - Items are stored as JSON documents with key, collection, and value fields

    By default, collections are not sanitized. This means that there are character and length restrictions on
    collection names that may cause errors when trying to get and put entries.

    To avoid issues, you may want to consider leveraging the `CosmosDBV1CollectionSanitizationStrategy` strategy.
    """

    _client: CosmosClient
    _database: DatabaseProxy | None
    _container: ContainerProxy | None
    _url: str
    _credential: str
    _database_name: str
    _container_name: str
    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        client: CosmosClient,
        database_name: str | None = None,
        container_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Cosmos DB store.

        Args:
            client: The CosmosClient to use.
            database_name: The name of the Cosmos DB database.
            container_name: The name of the Cosmos DB container.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
            auto_create: Whether to automatically create database/container if they don't exist. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        url: str,
        credential: str,
        database_name: str | None = None,
        container_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Cosmos DB store.

        Args:
            url: The Cosmos DB account URL.
            credential: The Cosmos DB account key or credential.
            database_name: The name of the Cosmos DB database.
            container_name: The name of the Cosmos DB container.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
            auto_create: Whether to automatically create database/container if they don't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        client: CosmosClient | None = None,
        url: str | None = None,
        credential: str | None = None,
        database_name: str | None = None,
        container_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Cosmos DB store.

        Args:
            client: The CosmosClient to use (mutually exclusive with url/credential). If provided, the store
                will not manage the client's lifecycle (will not close it). The caller is responsible for
                managing the client's lifecycle.
            url: The Cosmos DB account URL (mutually exclusive with client).
            credential: The Cosmos DB account key or credential (mutually exclusive with client).
            database_name: The name of the Cosmos DB database.
            container_name: The name of the Cosmos DB container.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
            auto_create: Whether to automatically create database/container if they don't exist. Defaults to True.
                When False, raises ValueError if the database or container doesn't exist.
        """
        client_provided = client is not None

        if client:
            self._client = client
        else:
            if not url or not credential:
                msg = "Either 'client' or both 'url' and 'credential' must be provided"
                raise ValueError(msg)
            self._client = _create_cosmosdb_client(url=url, credential=credential)

        self._database_name = database_name or DEFAULT_DATABASE
        self._container_name = container_name or DEFAULT_CONTAINER
        self._database = None
        self._container = None
        self._auto_create = auto_create

        super().__init__(
            default_collection=default_collection,
            collection_sanitization_strategy=collection_sanitization_strategy,
            client_provided_by_user=client_provided,
        )

    @override
    async def _setup(self) -> None:
        """Initialize the database and container."""
        # Register client cleanup if we own the client
        if not self._client_provided_by_user:
            await self._exit_stack.enter_async_context(self._client)

        if self._auto_create:
            self._database = await _create_database_if_not_exists(self._client, self._database_name)
            # Use -1 for default_ttl to enable per-item TTL without a default expiration
            self._container = await _create_container_if_not_exists(
                self._database,
                self._container_name,
                default_ttl=-1,
            )
        else:
            try:
                self._database = self._client.get_database_client(self._database_name)
                # Verify database exists by reading its properties
                _ = await self._database.read()
            except exceptions.CosmosResourceNotFoundError as e:
                msg = f"Database '{self._database_name}' does not exist. Either create the database manually or set auto_create=True."
                raise ValueError(msg) from e

            try:
                self._container = self._database.get_container_client(self._container_name)
                # Verify container exists by reading its properties
                _ = await self._container.read()
            except exceptions.CosmosResourceNotFoundError as e:
                msg = f"Container '{self._container_name}' does not exist. Either create the container manually or set auto_create=True."
                raise ValueError(msg) from e

    def _make_item_id(self, *, key: str, collection: str) -> str:  # noqa: ARG002 - collection reserved for future use
        """Create a unique item ID from key and collection.

        Cosmos DB requires a unique 'id' field within each partition.
        Since we use 'collection' as the partition key, the key alone is unique within a partition.

        Args:
            key: The key for the item.
            collection: The collection (partition key value).

        Returns:
            The item ID (just the key since it's unique within the partition).
        """
        return key

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        if self._container is None:
            msg = "Container not initialized"
            raise ValueError(msg)

        sanitized_collection = self._sanitize_collection(collection=collection)
        item_id = self._make_item_id(key=key, collection=sanitized_collection)

        item = await _read_item(self._container, item_id, partition_key=sanitized_collection)
        if not item:
            return None

        json_value = item.get("value")
        if not json_value:
            return None

        managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=json_value)

        # If the item has an expires_at field from Cosmos DB, use it
        if expires_at_str := item.get("expires_at"):
            managed_entry.expires_at = datetime.fromisoformat(expires_at_str)

        return managed_entry

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        if self._container is None:
            msg = "Container not initialized"
            raise ValueError(msg)

        sanitized_collection = self._sanitize_collection(collection=collection)

        # Build query for multiple keys using parameterized queries (safe from injection)
        key_placeholders = ", ".join([f"@key{i}" for i in range(len(keys))])
        query = f"SELECT * FROM c WHERE c.collection = @collection AND c.id IN ({key_placeholders})"  # noqa: S608 - parameterized query
        parameters: list[dict[str, Any]] = [{"name": "@collection", "value": sanitized_collection}]
        parameters.extend({"name": f"@key{i}", "value": key} for i, key in enumerate(keys))

        items = await _query_items(self._container, query, parameters, partition_key=sanitized_collection)

        # Create a mapping of key to item
        items_by_key: dict[str, dict[str, Any]] = {item["id"]: item for item in items}

        results: list[ManagedEntry | None] = []
        for key in keys:
            item = items_by_key.get(key)
            if not item:
                results.append(None)
                continue

            json_value = item.get("value")
            if not json_value:
                results.append(None)
                continue

            managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=json_value)

            if expires_at_str := item.get("expires_at"):
                managed_entry.expires_at = datetime.fromisoformat(expires_at_str)

            results.append(managed_entry)

        return results

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        if self._container is None:
            msg = "Container not initialized"
            raise ValueError(msg)

        sanitized_collection = self._sanitize_collection(collection=collection)
        item_id = self._make_item_id(key=key, collection=sanitized_collection)
        json_value = self._serialization_adapter.dump_json(entry=managed_entry, key=key, collection=collection)

        item: dict[str, Any] = {
            "id": item_id,
            "collection": sanitized_collection,
            "key": key,
            "value": json_value,
        }

        # Add TTL if present
        if managed_entry.expires_at is not None:
            # Store expires_at as ISO string for our own tracking
            item["expires_at"] = managed_entry.expires_at.isoformat()

            # Calculate TTL in seconds from now for Cosmos DB's TTL feature
            now = datetime.now(tz=timezone.utc)
            ttl_seconds = int((managed_entry.expires_at - now).total_seconds())
            if ttl_seconds > 0:
                item["ttl"] = ttl_seconds

        await _upsert_item(self._container, item)

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

        # Cosmos DB doesn't have native batch upsert in the Python SDK,
        # so we iterate through entries
        for key, managed_entry in zip(keys, managed_entries, strict=True):
            await self._put_managed_entry(
                key=key,
                collection=collection,
                managed_entry=managed_entry,
            )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        if self._container is None:
            msg = "Container not initialized"
            raise ValueError(msg)

        sanitized_collection = self._sanitize_collection(collection=collection)
        item_id = self._make_item_id(key=key, collection=sanitized_collection)

        return await _delete_item(self._container, item_id, partition_key=sanitized_collection)

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        if not keys:
            return 0

        deleted_count = 0
        for key in keys:
            if await self._delete_managed_entry(key=key, collection=collection):
                deleted_count += 1

        return deleted_count

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete all items in a collection.

        Note: This deletes all items with the given collection partition key,
        not the container itself.
        """
        if self._container is None:
            msg = "Container not initialized"
            raise ValueError(msg)

        sanitized_collection = self._sanitize_collection(collection=collection)

        # Query all items in the collection
        query = "SELECT c.id FROM c WHERE c.collection = @collection"
        parameters = [{"name": "@collection", "value": sanitized_collection}]

        items = await _query_items(self._container, query, parameters, partition_key=sanitized_collection)

        if not items:
            return False

        # Delete each item
        for item in items:
            _ = await _delete_item(self._container, item["id"], partition_key=sanitized_collection)

        return True

    # No need to override _close - the exit stack handles all cleanup automatically
