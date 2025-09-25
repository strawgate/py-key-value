from pathlib import Path
from typing import Any, overload

from diskcache import Cache
from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

DEFAULT_DISK_STORE_SIZE_LIMIT = 1 * 1024 * 1024 * 1024  # 1GB


class DiskStore(BaseManagedKVStore):
    """A disk-based store that uses the diskcache library to store data. The diskcache library is a syncronous implementation of an LRU
    cache and may not be suitable for high-traffic applications."""

    _cache: Cache

    @overload
    def __init__(self, *, disk_cache: Cache) -> None:
        """Initialize the disk cache.

        Args:
            disk_cache: An existing diskcache Cache instance to use.
        """

    @overload
    def __init__(self, *, directory: Path | str, size_limit: int | None = None) -> None:
        """Initialize the disk cache.

        Args:
            directory: The directory to use for the disk cache.
            size_limit: The maximum size of the disk cache. Defaults to 1GB.
        """

    def __init__(self, *, disk_cache: Cache | None = None, directory: Path | str | None = None, size_limit: int | None = None) -> None:
        """Initialize the in-memory cache.

        Args:
            disk_cache: An existing diskcache Cache instance to use.
            directory: The directory to use for the disk cache.
            size_limit: The maximum size of the disk cache. Defaults to 1GB.
        """
        if isinstance(directory, Path):
            directory = str(object=directory)

        self._cache = disk_cache or Cache(directory=directory, size_limit=size_limit or DEFAULT_DISK_STORE_SIZE_LIMIT)

        super().__init__()

    @override
    async def setup(self) -> None:
        pass

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        cache_entry: Any = self._cache.get(combo_key)  # pyright: ignore[reportAny]

        if not isinstance(cache_entry, str):
            return None

        return ManagedEntry.from_json(json_str=cache_entry)

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

        _ = self._cache.set(key=combo_key, value=cache_entry.to_json(), expire=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        return self._cache.delete(key=combo_key)

    @override
    async def keys(self, collection: str) -> list[str]:
        compound_strings: list[str] = list(self._cache.iterkeys())

        return get_keys_from_compound_keys(compound_keys=compound_strings, collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        cleared_count: int = 0

        for key in await self.keys(collection=collection):
            _ = await self.delete(collection=collection, key=key)
            cleared_count += 1

        return cleared_count

    @override
    async def list_collections(self) -> list[str]:
        compound_strings: list[str] = list(self._cache.iterkeys())
        return get_collections_from_compound_keys(compound_keys=compound_strings)

    @override
    async def cull(self) -> None:
        _ = self._cache.cull()
