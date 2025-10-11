import asyncio
import contextlib
import json

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseDestroyCollectionStore,
    BaseDestroyStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
    BaseStore,
)

try:
    import secretstorage
    from jeepney.io.blocking import DBusConnection
except ImportError as e:
    msg = "LibsecretStore requires py-key-value-aio[libsecret]"
    raise ImportError(msg) from e


DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000


class LibsecretStore(
    BaseDestroyStore,
    BaseDestroyCollectionStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
    BaseStore,
):
    """Linux libsecret-based key-value store using the Secret Service API."""

    _connection: DBusConnection | None
    _collection: secretstorage.Collection | None

    def __init__(
        self,
        *,
        connection: DBusConnection | None = None,
        collection_name: str = "py-key-value",
        default_collection: str | None = None,
    ) -> None:
        """Initialize the libsecret store.

        Args:
            connection: An existing DBus connection to use. If not provided, a new connection will be created.
            collection_name: The name of the Secret Service collection to use. Defaults to "py-key-value".
            default_collection: The default collection to use if no collection is provided.
        """
        self._connection = connection
        self._collection = None
        self._collection_name = collection_name
        self._owns_connection = connection is None

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Initialize the Secret Service connection and collection."""

        def _init_connection() -> tuple[DBusConnection, secretstorage.Collection]:
            connection = secretstorage.dbus_init() if self._connection is None else self._connection

            # Try to get existing collection or create a new one
            collection = secretstorage.get_collection_by_alias(connection, self._collection_name)
            if collection is None:
                collection = secretstorage.create_collection(connection, self._collection_name)

            # Unlock collection if needed
            if collection.is_locked():
                collection.unlock()

            return connection, collection

        self._connection, self._collection = await asyncio.to_thread(_init_connection)

    def _make_attributes(self, *, key: str, collection: str) -> dict[str, str]:
        """Create attributes dictionary for secret storage."""
        return {
            "py-key-value-collection": collection,
            "py-key-value-key": key,
        }

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Get a managed entry from the store."""

        def _get_item() -> ManagedEntry | None:
            if self._collection is None:
                return None

            attributes = self._make_attributes(key=key, collection=collection)
            items = list(self._collection.search_items(attributes))

            if not items:
                return None

            # Get the first matching item
            item = items[0]
            secret_bytes = item.get_secret()

            # Deserialize the managed entry
            try:
                return ManagedEntry.from_json(json_str=secret_bytes.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return None

        return await asyncio.to_thread(_get_item)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Put a managed entry into the store."""

        def _put_item() -> None:
            if self._collection is None:
                return

            attributes = self._make_attributes(key=key, collection=collection)
            label = compound_key(collection=collection, key=key)
            secret_json = managed_entry.to_json(include_expiration=True)
            secret_bytes = secret_json.encode("utf-8")

            # Replace existing item if it exists
            self._collection.create_item(
                label=label,
                attributes=attributes,
                secret=secret_bytes,
                replace=True,
            )

        await asyncio.to_thread(_put_item)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from the store."""

        def _delete_item() -> bool:
            if self._collection is None:
                return False

            attributes = self._make_attributes(key=key, collection=collection)
            items = list(self._collection.search_items(attributes))

            if not items:
                return False

            # Delete all matching items (should only be one due to replace=True)
            for item in items:
                item.delete()

            return True

        return await asyncio.to_thread(_delete_item)

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        """Get all keys in a collection."""

        def _get_keys() -> list[str]:
            if self._collection is None:
                return []

            attributes = {"py-key-value-collection": collection}
            items = self._collection.search_items(attributes)

            keys = []
            for item in items:
                item_attrs = item.get_attributes()
                if "py-key-value-key" in item_attrs:
                    keys.append(item_attrs["py-key-value-key"])

                if limit and len(keys) >= limit:
                    break

            return keys

        return await asyncio.to_thread(_get_keys)

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """Get all collection names."""

        def _get_collections() -> list[str]:
            if self._collection is None:
                return []

            all_items = self._collection.get_all_items()
            collections = set()

            for item in all_items:
                attrs = item.get_attributes()
                if "py-key-value-collection" in attrs:
                    collections.add(attrs["py-key-value-collection"])

                if limit and len(collections) >= limit:
                    break

            return sorted(collections)

        return await asyncio.to_thread(_get_collections)

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete all items in a collection."""

        def _delete_items() -> bool:
            if self._collection is None:
                return False

            attributes = {"py-key-value-collection": collection}
            items = list(self._collection.search_items(attributes))

            if not items:
                return False

            for item in items:
                item.delete()

            return True

        return await asyncio.to_thread(_delete_items)

    @override
    async def _delete_store(self) -> bool:
        """Delete all items in the store."""

        def _delete_all_items() -> bool:
            if self._collection is None:
                return False

            items = list(self._collection.get_all_items())

            if not items:
                return False

            # Only delete items that belong to py-key-value
            deleted = False
            for item in items:
                attrs = item.get_attributes()
                if "py-key-value-collection" in attrs and "py-key-value-key" in attrs:
                    item.delete()
                    deleted = True

            return deleted

        return await asyncio.to_thread(_delete_all_items)

    async def _close(self) -> None:
        """Close the DBus connection if we own it."""

        def _close_connection() -> None:
            if self._owns_connection and self._connection is not None:
                self._connection.close()

        await asyncio.to_thread(_close_connection)

    def __del__(self) -> None:
        """Clean up the connection on deletion."""
        if self._owns_connection and self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.close()
