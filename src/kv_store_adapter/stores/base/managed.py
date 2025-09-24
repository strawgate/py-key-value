"""
Base abstract class for managed key-value store implementations.
"""

import asyncio
from abc import ABC, abstractmethod
from asyncio.locks import Lock
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from typing_extensions import override

from kv_store_adapter.errors import SetupError
from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry
from kv_store_adapter.stores.utils.time_to_live import calculate_expires_at
from kv_store_adapter.types import TTLInfo


class BaseManagedKVStore(BaseKVStore, ABC):
    """An opinionated Abstract base class for managed key-value stores using ManagedEntry objects.

    This class handles TTL management, expiration checking, and entry wrapping automatically.
    Implementations only need to handle storage and retrieval of ManagedEntry objects and culling of expired entries.
    """

    _setup_complete: bool
    _setup_lock: asyncio.Lock

    _setup_collection_locks: defaultdict[str, Lock]
    _setup_collection_complete: defaultdict[str, bool]

    def __init__(self) -> None:
        self._setup_complete = False
        self._setup_lock = asyncio.Lock()
        self._setup_collection_locks = defaultdict[str, asyncio.Lock](asyncio.Lock)
        self._setup_collection_complete = defaultdict[str, bool](bool)

    async def setup(self) -> None:
        """Initialize the store (called once before first use)."""

    async def setup_collection(self, collection: str) -> None:  # pyright: ignore[reportUnusedParameter]
        """Initialize the collection (called once before first use of the collection)."""

    async def setup_collection_once(self, collection: str) -> None:
        await self.setup_once()

        if not self._setup_collection_complete[collection]:
            async with self._setup_collection_locks[collection]:
                if not self._setup_collection_complete[collection]:
                    try:
                        await self.setup_collection(collection=collection)
                    except Exception as e:
                        raise SetupError(message=f"Failed to setup collection: {e}", extra_info={"collection": collection}) from e
                    self._setup_collection_complete[collection] = True

    async def setup_once(self) -> None:
        if not self._setup_complete:
            async with self._setup_lock:
                if not self._setup_complete:
                    try:
                        await self.setup()
                    except Exception as e:
                        raise SetupError(message=f"Failed to setup store: {e}", extra_info={"store": self.__class__.__name__}) from e
                    self._setup_complete = True

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        """Retrieve a non-expired value by key from the specified collection."""
        await self.setup_collection_once(collection=collection)

        if cache_entry := await self.get_entry(collection=collection, key=key):
            if cache_entry.is_expired:
                # _ = await self.delete(collection=collection, key=key)
                return None

            return cache_entry.value
        return None

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        await self.setup_collection_once(collection=collection)

        if cache_entry := await self.get_entry(collection=collection, key=key):
            return cache_entry.to_ttl_info()

        return None

    @abstractmethod
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a cache entry by key from the specified collection."""

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        """Store a key-value pair in the specified collection with optional TTL."""
        await self.setup_collection_once(collection=collection)

        created_at: datetime = datetime.now(tz=timezone.utc)

        cache_entry: ManagedEntry = ManagedEntry(
            created_at=created_at,
            expires_at=calculate_expires_at(created_at=created_at, ttl=ttl),
            ttl=ttl,
            collection=collection,
            key=key,
            value=value,
        )

        await self.put_entry(collection=collection, key=key, cache_entry=cache_entry, ttl=ttl)

    @abstractmethod
    async def put_entry(self, collection: str, key: str, cache_entry: ManagedEntry, *, ttl: float | None = None) -> None:
        """Store a managed entry by key in the specified collection."""
        ...

    @override
    async def exists(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        return await super().exists(collection=collection, key=key)
