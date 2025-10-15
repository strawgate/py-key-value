"""
Base abstract class for managed key-value store implementations.
"""

from abc import ABC, abstractmethod
from asyncio.locks import Lock
from collections import defaultdict
from collections.abc import Sequence
from types import TracebackType
from typing import Any, SupportsFloat

from key_value.shared.constants import DEFAULT_COLLECTION_NAME
from key_value.shared.errors import StoreSetupError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.time_to_live import now, prepare_ttl, prepare_ttls
from typing_extensions import Self, override

from key_value.aio.protocols.key_value import (
    AsyncCullProtocol,
    AsyncDestroyCollectionProtocol,
    AsyncDestroyStoreProtocol,
    AsyncEnumerateCollectionsProtocol,
    AsyncEnumerateKeysProtocol,
    AsyncKeyValueProtocol,
)


class BaseStore(AsyncKeyValueProtocol, ABC):
    """An opinionated Abstract base class for managed key-value stores using ManagedEntry objects.

    This class implements all of the methods required for compliance with the KVStore protocol but
    requires subclasses to implement the _get_managed_entry, _put_managed_entry, and _delete_managed_entry methods.

    Subclasses can also override the _get_managed_entries, _put_managed_entries, and _delete_managed_entries methods if desired.

    Subclasses can implement the _setup, which will be called once before the first use of the store, and _setup_collection, which will
    be called once per collection before the first use of a collection.
    """

    _setup_complete: bool
    _setup_lock: Lock

    _setup_collection_locks: defaultdict[str, Lock]
    _setup_collection_complete: defaultdict[str, bool]

    default_collection: str

    def __init__(self, *, default_collection: str | None = None) -> None:
        """Initialize the managed key-value store.

        Args:
            default_collection: The default collection to use if no collection is provided.
                Defaults to "default_collection".
        """

        self._setup_complete = False
        self._setup_lock = Lock()
        self._setup_collection_locks = defaultdict(Lock)
        self._setup_collection_complete = defaultdict(bool)

        self.default_collection = default_collection or DEFAULT_COLLECTION_NAME

        super().__init__()

    async def _setup(self) -> None:
        """Initialize the store (called once before first use)."""

    async def _setup_collection(self, *, collection: str) -> None:  # pyright: ignore[reportUnusedParameter]
        """Initialize the collection (called once before first use of the collection)."""

    async def setup(self) -> None:
        if not self._setup_complete:
            async with self._setup_lock:
                if not self._setup_complete:
                    try:
                        await self._setup()
                    except Exception as e:
                        raise StoreSetupError(
                            message=f"Failed to setup key value store: {e}", extra_info={"store": self.__class__.__name__}
                        ) from e
                    self._setup_complete = True

    async def setup_collection(self, *, collection: str) -> None:
        await self.setup()

        if not self._setup_collection_complete[collection]:
            async with self._setup_collection_locks[collection]:
                if not self._setup_collection_complete[collection]:
                    try:
                        await self._setup_collection(collection=collection)
                    except Exception as e:
                        raise StoreSetupError(message=f"Failed to setup collection: {e}", extra_info={"collection": collection}) from e
                    self._setup_collection_complete[collection] = True

    @abstractmethod
    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a cache entry by key from the specified collection."""

    async def _get_managed_entries(self, *, collection: str, keys: list[str]) -> list[ManagedEntry | None]:
        """Retrieve multiple managed entries by key from the specified collection."""

        return [await self._get_managed_entry(collection=collection, key=key) for key in keys]

    @override
    async def get(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve a value by key from the specified collection.

        Args:
            collection: The collection to retrieve the value from. If no collection is provided, it will use the default collection.
            key: The key to retrieve the value from.

        Returns:
            The value associated with the key, or None if not found or expired.
        """
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        managed_entry: ManagedEntry | None = await self._get_managed_entry(collection=collection, key=key)

        if not managed_entry:
            return None

        if managed_entry.is_expired:
            return None

        return managed_entry.value

    @override
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        entries = await self._get_managed_entries(keys=keys, collection=collection)
        return [entry.value if entry and not entry.is_expired else None for entry in entries]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        managed_entry: ManagedEntry | None = await self._get_managed_entry(collection=collection, key=key)

        if not managed_entry or managed_entry.is_expired:
            return (None, None)

        return (managed_entry.value, managed_entry.ttl)

    @override
    async def ttl_many(
        self,
        keys: list[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[dict[str, Any] | None, float | None]]:
        """Retrieve multiple values and TTLs by key from the specified collection.

        Returns a list of tuples of the form (value, ttl_seconds). Missing or expired
        entries are represented as (None, None).
        """
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        entries = await self._get_managed_entries(keys=keys, collection=collection)
        return [(entry.value, entry.ttl) if entry and not entry.is_expired else (None, None) for entry in entries]

    @abstractmethod
    async def _put_managed_entry(self, *, collection: str, key: str, managed_entry: ManagedEntry) -> None:
        """Store a managed entry by key in the specified collection."""
        ...

    async def _put_managed_entries(self, *, collection: str, keys: list[str], managed_entries: Sequence[ManagedEntry]) -> None:
        """Store multiple managed entries by key in the specified collection."""

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            await self._put_managed_entry(
                collection=collection,
                key=key,
                managed_entry=managed_entry,
            )

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        """Store a key-value pair in the specified collection with optional TTL."""
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        managed_entry: ManagedEntry = ManagedEntry(value=value, ttl=prepare_ttl(t=ttl), created_at=now())

        await self._put_managed_entry(
            collection=collection,
            key=key,
            managed_entry=managed_entry,
        )

    def _prepare_put_many(
        self, *, keys: list[str], values: Sequence[dict[str, Any]], ttl: Sequence[SupportsFloat | None] | SupportsFloat | None
    ) -> tuple[list[str], Sequence[dict[str, Any]], list[float | None]]:
        """Prepare multiple managed entries for a put_many operation.

        Inheriting classes can use this method if they need to modify a put_many operation."""

        if len(keys) != len(values):
            msg = "put_many called but a different number of keys and values were provided"
            raise ValueError(msg) from None

        if ttl and isinstance(ttl, Sequence) and len(ttl) != len(keys):
            msg = "put_many called but a different number of keys and ttl values were provided"
            raise ValueError(msg) from None

        ttl_for_entries: list[float | None] = prepare_ttls(t=ttl, count=len(keys))

        return (keys, values, ttl_for_entries)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | None = None,
    ) -> None:
        """Store multiple key-value pairs in the specified collection."""

        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        keys, values, ttl_for_entries = self._prepare_put_many(keys=keys, values=values, ttl=ttl)

        managed_entries: list[ManagedEntry] = [
            ManagedEntry(value=value, ttl=ttl_for_entries[i], created_at=now()) for i, value in enumerate(values)
        ]

        await self._put_managed_entries(collection=collection, keys=keys, managed_entries=managed_entries)

    @abstractmethod
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry by key from the specified collection."""
        ...

    async def _delete_managed_entries(self, *, keys: list[str], collection: str) -> int:
        """Delete multiple managed entries by key from the specified collection."""

        deleted_count: int = 0

        for key in keys:
            if await self._delete_managed_entry(key=key, collection=collection):
                deleted_count += 1

        return deleted_count

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        return await self._delete_managed_entry(key=key, collection=collection)

    @override
    async def delete_many(self, keys: list[str], *, collection: str | None = None) -> int:
        """Delete multiple managed entries by key from the specified collection."""
        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        return await self._delete_managed_entries(keys=keys, collection=collection)


class BaseEnumerateKeysStore(BaseStore, AsyncEnumerateKeysProtocol, ABC):
    """An abstract base class for enumerate key-value stores.

    Subclasses must implement the _get_collection_keys method.
    """

    @override
    async def keys(self, collection: str | None = None, *, limit: int | None = None) -> list[str]:
        """List all keys in the specified collection."""

        collection = collection or self.default_collection
        await self.setup_collection(collection=collection)

        return await self._get_collection_keys(collection=collection, limit=limit)

    @abstractmethod
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        """List all keys in the specified collection."""


class BaseContextManagerStore(BaseStore, ABC):
    """An abstract base class for context manager stores."""

    async def __aenter__(self) -> Self:
        await self.setup()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self._close()

    async def close(self) -> None:
        await self._close()

    @abstractmethod
    async def _close(self) -> None:
        """Close the store."""
        ...


class BaseEnumerateCollectionsStore(BaseStore, AsyncEnumerateCollectionsProtocol, ABC):
    """An abstract base class for enumerate collections stores.

    Subclasses must implement the _get_collection_names method.
    """

    @override
    async def collections(self, *, limit: int | None = None) -> list[str]:
        """List all available collection names (may include empty collections)."""
        await self.setup()

        return await self._get_collection_names(limit=limit)

    @abstractmethod
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """List all available collection names (may include empty collections)."""


class BaseDestroyStore(BaseStore, AsyncDestroyStoreProtocol, ABC):
    """An abstract base class for destroyable stores.

    Subclasses must implement the _delete_store method.
    """

    @override
    async def destroy(self) -> bool:
        """Destroy the store."""
        await self.setup()

        return await self._delete_store()

    @abstractmethod
    async def _delete_store(self) -> bool:
        """Delete the store."""
        ...


class BaseDestroyCollectionStore(BaseStore, AsyncDestroyCollectionProtocol, ABC):
    """An abstract base class for destroyable collections.

    Subclasses must implement the _delete_collection method.
    """

    @override
    async def destroy_collection(self, collection: str) -> bool:
        """Destroy the collection."""
        await self.setup()

        return await self._delete_collection(collection=collection)

    @abstractmethod
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete the collection."""
        ...


class BaseCullStore(BaseStore, AsyncCullProtocol, ABC):
    """An abstract base class for cullable stores.

    Subclasses must implement the _cull method.
    """

    @override
    async def cull(self) -> None:
        """Cull the store."""
        await self.setup()

        return await self._cull()

    @abstractmethod
    async def _cull(self) -> None:
        """Cull the store."""
        ...
