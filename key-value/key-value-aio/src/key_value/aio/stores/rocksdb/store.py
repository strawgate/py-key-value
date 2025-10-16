from pathlib import Path
from typing import overload

from key_value.shared.utils.compound import compound_key, get_keys_from_compound_keys
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyStore, BaseEnumerateKeysStore, BaseStore

try:
    import rocksdb
except ImportError as e:
    msg = "RocksDBStore requires py-key-value-aio[rocksdb]"
    raise ImportError(msg) from e


class RocksDBStore(BaseDestroyStore, BaseEnumerateKeysStore, BaseContextManagerStore, BaseStore):
    """A RocksDB-based key-value store."""

    _db: rocksdb.DB

    @overload
    def __init__(self, *, db: rocksdb.DB, default_collection: str | None = None) -> None:
        """Initialize the RocksDB store.

        Args:
            db: An existing RocksDB database instance to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, path: Path | str, default_collection: str | None = None, **options: dict) -> None:
        """Initialize the RocksDB store.

        Args:
            path: The path to the RocksDB database directory.
            default_collection: The default collection to use if no collection is provided.
            options: Additional options to pass to RocksDB.
        """

    def __init__(
        self,
        *,
        db: rocksdb.DB | None = None,
        path: Path | str | None = None,
        default_collection: str | None = None,
        **options: dict,
    ) -> None:
        """Initialize the RocksDB store.

        Args:
            db: An existing RocksDB database instance to use.
            path: The path to the RocksDB database directory.
            default_collection: The default collection to use if no collection is provided.
            options: Additional options to pass to RocksDB.
        """
        if db is not None and path is not None:
            msg = "Provide only one of db or path"
            raise ValueError(msg)

        if db is None and path is None:
            msg = "Either db or path must be provided"
            raise ValueError(msg)

        if db:
            self._db = db
        elif path:
            path = Path(path)
            path.mkdir(parents=True, exist_ok=True)

            opts = rocksdb.Options()
            opts.create_if_missing = True
            opts.max_open_files = 300000
            opts.write_buffer_size = 67108864
            opts.max_write_buffer_number = 3
            opts.target_file_size_base = 67108864

            # Apply any additional options passed
            for key, value in options.items():
                setattr(opts, key, value)

            self._db = rocksdb.DB(str(path), opts)

        super().__init__(default_collection=default_collection)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        value: bytes | None = self._db.get(combo_key.encode("utf-8"))

        if value is None:
            return None

        managed_entry_str: str = value.decode("utf-8")
        managed_entry: ManagedEntry = ManagedEntry.from_json(json_str=managed_entry_str)

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
        json_value: str = managed_entry.to_json()

        self._db.put(combo_key.encode("utf-8"), json_value.encode("utf-8"))

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        combo_key_bytes = combo_key.encode("utf-8")

        # Check if key exists before deleting
        exists = self._db.get(combo_key_bytes) is not None

        if exists:
            self._db.delete(combo_key_bytes)

        return exists

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        pattern = compound_key(collection=collection, key="")
        pattern_bytes = pattern.encode("utf-8")

        keys: list[str] = []
        it = self._db.iterkeys()
        it.seek(pattern_bytes)

        count = 0
        for key_bytes in it:
            key = key_bytes.decode("utf-8")
            if not key.startswith(pattern):
                break

            keys.append(key)
            count += 1

            if limit is not None and count >= limit:
                break

        return get_keys_from_compound_keys(compound_keys=keys, collection=collection)

    @override
    async def _delete_store(self) -> bool:
        # Delete all keys in the database
        batch = rocksdb.WriteBatch()
        it = self._db.iterkeys()
        it.seek_to_first()

        for key in it:
            batch.delete(key)

        self._db.write(batch)
        return True

    @override
    async def _close(self) -> None:
        del self._db

    def __del__(self) -> None:
        if hasattr(self, "_db"):
            del self._db
