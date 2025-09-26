import time
from pathlib import Path
from typing import overload

from typing_extensions import override

from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.stores.utils.compound import compound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

try:
    from diskcache import Cache
except ImportError as e:
    msg = "DiskStore requires py-kv-store-adapter[disk]"
    raise ImportError(msg) from e

DEFAULT_DISK_STORE_MAX_SIZE = 1 * 1024 * 1024 * 1024  # 1GB


class DiskStore(BaseStore):
    """A disk-based store that uses the diskcache library to store data."""

    _cache: Cache

    @overload
    def __init__(self, *, disk_cache: Cache, default_collection: str | None = None) -> None:
        """Initialize the disk cache.

        Args:
            disk_cache: An existing diskcache Cache instance to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, directory: Path | str, max_size: int | None = None, default_collection: str | None = None) -> None:
        """Initialize the disk cache.

        Args:
            directory: The directory to use for the disk cache.
            max_size: The maximum size of the disk cache. Defaults to 1GB.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        disk_cache: Cache | None = None,
        directory: Path | str | None = None,
        max_size: int | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the disk cache.

        Args:
            disk_cache: An existing diskcache Cache instance to use.
            directory: The directory to use for the disk cache.
            max_size: The maximum size of the disk cache. Defaults to 1GB.
            default_collection: The default collection to use if no collection is provided.
        """
        if isinstance(directory, Path):
            directory = str(object=directory)

        self._cache = disk_cache or Cache(directory=directory, size_limit=max_size or DEFAULT_DISK_STORE_MAX_SIZE)

        super().__init__(default_collection=default_collection)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        expire_epoch: float | None

        managed_entry_str, expire_epoch = self._cache.get(key=combo_key, expire_time=True)  # pyright: ignore[reportAny]

        if not isinstance(managed_entry_str, str):
            return None

        ttl = (expire_epoch - time.time()) if expire_epoch else None

        managed_entry: ManagedEntry = ManagedEntry.from_json(json_str=managed_entry_str, ttl=ttl)

        return managed_entry

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        _ = self._cache.set(key=combo_key, value=managed_entry.to_json(), expire=managed_entry.ttl)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return self._cache.delete(key=combo_key, retry=True)
