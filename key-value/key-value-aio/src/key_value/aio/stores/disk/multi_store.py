import time
from collections.abc import Callable
from pathlib import Path
from typing import overload

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore

try:
    from diskcache import Cache
    from pathvalidate import sanitize_filename
except ImportError as e:
    msg = "DiskStore requires py-key-value-aio[disk]"
    raise ImportError(msg) from e

DEFAULT_DISK_STORE_SIZE_LIMIT = 1 * 1024 * 1024 * 1024  # 1GB

CacheFactory = Callable[[str], Cache]


def _sanitize_collection_for_filesystem(collection: str) -> str:
    """Sanitize the collection name so that it can be used as a directory name on the filesystem."""

    return sanitize_filename(filename=collection)


class MultiDiskStore(BaseContextManagerStore, BaseStore):
    """A disk-based store that uses the diskcache library to store data. The MultiDiskStore creates one diskcache Cache
    instance per collection."""

    _cache: dict[str, Cache]

    _disk_cache_factory: CacheFactory

    _base_directory: Path

    _max_size: int | None

    @overload
    def __init__(self, *, disk_cache_factory: CacheFactory, default_collection: str | None = None) -> None:
        """Initialize the disk caches.

        Args:
            disk_cache_factory: A factory function that creates a diskcache Cache instance for a given collection.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, base_directory: Path, max_size: int | None = None, default_collection: str | None = None) -> None:
        """Initialize the disk caches.

        Args:
            base_directory: The directory to use for the disk caches.
            max_size: The maximum size of the disk caches. Defaults to 1GB.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        disk_cache_factory: CacheFactory | None = None,
        base_directory: Path | None = None,
        max_size: int | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the disk caches.

        Args:
            disk_cache_factory: A factory function that creates a diskcache Cache instance for a given collection.
            base_directory: The directory to use for the disk caches.
            max_size: The maximum size of the disk caches. Defaults to 1GB.
            default_collection: The default collection to use if no collection is provided.
        """
        if disk_cache_factory is None and base_directory is None:
            msg = "Either disk_cache_factory or base_directory must be provided"
            raise ValueError(msg)

        if base_directory is None:
            base_directory = Path.cwd()

        self._max_size = max_size

        self._base_directory = base_directory.resolve()

        def default_disk_cache_factory(collection: str) -> Cache:
            sanitized_collection: str = _sanitize_collection_for_filesystem(collection=collection)

            cache_directory: Path = self._base_directory / sanitized_collection

            cache_directory.mkdir(parents=True, exist_ok=True)

            return Cache(directory=cache_directory, size_limit=self._max_size or DEFAULT_DISK_STORE_SIZE_LIMIT)

        self._disk_cache_factory = disk_cache_factory or default_disk_cache_factory

        self._cache = {}

        super().__init__(default_collection=default_collection)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        self._cache[collection] = self._disk_cache_factory(collection)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        expire_epoch: float

        managed_entry_str, expire_epoch = self._cache[collection].get(key=combo_key, expire_time=True)  # pyright: ignore[reportAny]

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

        _ = self._cache[collection].set(key=combo_key, value=managed_entry.to_json(include_expiration=False), expire=managed_entry.ttl)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return self._cache[collection].delete(key=combo_key, retry=True)

    def _sync_close(self) -> None:
        for cache in self._cache.values():
            cache.close()

    @override
    async def _close(self) -> None:
        self._sync_close()

    def __del__(self) -> None:
        self._sync_close()
