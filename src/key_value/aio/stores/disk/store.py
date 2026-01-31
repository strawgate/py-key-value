from datetime import datetime, timezone
from pathlib import Path
from typing import overload

from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore
from key_value.shared.compound import compound_key
from key_value.shared.managed_entry import ManagedEntry

try:
    from diskcache import Cache
except ImportError as e:
    msg = "DiskStore requires py-key-value-aio[disk]"
    raise ImportError(msg) from e


class DiskStore(BaseContextManagerStore, BaseStore):
    """A disk-based store that uses the diskcache library to store data."""

    _cache: Cache
    _auto_create: bool

    @overload
    def __init__(self, *, disk_cache: Cache, default_collection: str | None = None, auto_create: bool = True) -> None:
        """Initialize the disk store.

        Args:
            disk_cache: An existing diskcache Cache instance to use.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
        """

    @overload
    def __init__(
        self, *, directory: Path | str, max_size: int | None = None, default_collection: str | None = None, auto_create: bool = True
    ) -> None:
        """Initialize the disk store.

        Args:
            directory: The directory to use for the disk store.
            max_size: The maximum size of the disk store. Defaults to an unlimited size disk store
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        disk_cache: Cache | None = None,
        directory: Path | str | None = None,
        max_size: int | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the disk store.

        Args:
            disk_cache: An existing diskcache Cache instance to use. If provided, the store will
                not manage the cache's lifecycle (will not close it). The caller is responsible
                for managing the cache's lifecycle.
            directory: The directory to use for the disk store.
            max_size: The maximum size of the disk store.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
                When False, raises ValueError if the directory doesn't exist.
        """
        if disk_cache is not None and directory is not None:
            msg = "Provide only one of disk_cache or directory"
            raise ValueError(msg)

        if disk_cache is None and directory is None:
            msg = "Either disk_cache or directory must be provided"
            raise ValueError(msg)

        client_provided = disk_cache is not None
        self._client_provided_by_user = client_provided
        self._auto_create = auto_create

        if disk_cache:
            self._cache = disk_cache
        elif directory:
            directory = Path(directory)

            if not directory.exists():
                if not self._auto_create:
                    msg = f"Directory '{directory}' does not exist. Either create the directory manually or set auto_create=True."
                    raise ValueError(msg)
                directory.mkdir(parents=True, exist_ok=True)

            if max_size is not None and max_size > 0:
                self._cache = Cache(directory=directory, size_limit=max_size)
            else:
                self._cache = Cache(directory=directory, eviction_policy="none")

        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided,
            stable_api=True,
        )

    @override
    async def _setup(self) -> None:
        """Register cache cleanup if we own the cache."""
        if not self._client_provided_by_user:
            self._exit_stack.callback(self._cache.close)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        expire_epoch: float | None

        managed_entry_str, expire_epoch = self._cache.get(key=combo_key, expire_time=True)

        if not isinstance(managed_entry_str, str):
            return None

        managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=managed_entry_str)

        if expire_epoch:
            managed_entry.expires_at = datetime.fromtimestamp(expire_epoch, tz=timezone.utc)

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

        _ = self._cache.set(
            key=combo_key,
            value=self._serialization_adapter.dump_json(entry=managed_entry, key=key, collection=collection),
            expire=managed_entry.ttl,
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return self._cache.delete(key=combo_key, retry=True)

    def __del__(self) -> None:
        if not getattr(self, "_client_provided_by_user", False) and hasattr(self, "_cache"):
            self._cache.close()
