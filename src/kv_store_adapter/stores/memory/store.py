import sys
from typing import Any

from cachetools import TLRUCache
from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, uncompound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


def _memory_cache_ttu(_key: Any, value: ManagedEntry, now: float) -> float:  # pyright: ignore[reportAny]
    """Calculate time-to-use for cache entries based on their TTL."""
    return now + value.ttl if value.ttl else sys.maxsize


def _memory_cache_getsizeof(value: ManagedEntry) -> int:  # pyright: ignore[reportUnusedParameter]  # noqa: ARG001
    """Return size of cache entry (always 1 for entry counting)."""
    return 1


DEFAULT_MEMORY_CACHE_LIMIT = 1000


class MemoryStore(BaseManagedKVStore):
    """In-memory key-value store using TLRU (Time-aware Least Recently Used) cache."""

    max_entries: int
    _cache: TLRUCache[str, ManagedEntry]

    def __init__(self, max_entries: int = DEFAULT_MEMORY_CACHE_LIMIT):
        """Initialize the in-memory cache.

        Args:
            max_entries: The maximum number of entries to store in the cache. Defaults to 1000.
        """
        self.max_entries = max_entries
        self._cache = TLRUCache[str, ManagedEntry](
            maxsize=max_entries,
            ttu=_memory_cache_ttu,
            getsizeof=_memory_cache_getsizeof,
        )

        super().__init__()

    @override
    async def setup(self) -> None:
        pass

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        if cache_entry := self._cache.get(combo_key):
            return cache_entry

        return None

    @override
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)
        self._cache[combo_key] = cache_entry

    @override
    async def delete(self, collection: str, key: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        return self._cache.pop(combo_key, None) is not None

    @override
    async def keys(self, collection: str) -> list[str]:
        keys: list[str] = []

        for key in self._cache:
            entry_collection, entry_key = uncompound_key(key=key)
            if entry_collection == collection:
                keys.append(entry_key)

        return keys

    @override
    async def clear_collection(self, collection: str) -> int:
        cleared_count: int = 0

        for key in await self.keys(collection=collection):
            _ = await self.delete(collection=collection, key=key)
            cleared_count += 1

        return cleared_count

    @override
    async def list_collections(self) -> list[str]:
        collections: set[str] = set()
        for key in self._cache:
            entry_collection, _ = uncompound_key(key=key)
            collections.add(entry_collection)
        return list(collections)

    @override
    async def cull(self) -> None:
        for collection in await self.list_collections():
            for key in await self.keys(collection=collection):
                _ = await self.get_entry(collection=collection, key=key)
