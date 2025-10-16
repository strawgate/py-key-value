from pathlib import Path
from typing import Any, overload

from key_value.shared.errors.store import KeyValueStoreError
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore

try:
    from rocksdict import Options, Rdict
except ImportError as e:
    msg = "RocksDBStore requires py-key-value-aio[rocksdb]"
    raise ImportError(msg) from e


class RocksDBStore(BaseContextManagerStore, BaseStore):
    """A RocksDB-based key-value store."""

    _db: Rdict
    _is_closed: bool

    @overload
    def __init__(self, *, db: Rdict, default_collection: str | None = None) -> None:
        """Initialize the RocksDB store.

        Args:
            db: An existing Rdict database instance to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, path: Path | str, default_collection: str | None = None) -> None:
        """Initialize the RocksDB store.

        Args:
            path: The path to the RocksDB database directory.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        db: Rdict | None = None,
        path: Path | str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the RocksDB store.

        Args:
            db: An existing Rdict database instance to use.
            path: The path to the RocksDB database directory.
            default_collection: The default collection to use if no collection is provided.
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

            opts = Options()
            opts.create_if_missing(True)

            self._db = Rdict(str(path), options=opts)

        self._is_closed = False

        super().__init__(default_collection=default_collection)

    @override
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # pyright: ignore[reportAny]
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self._close()

    @override
    async def _close(self) -> None:
        self._close_and_flush()

    def _close_and_flush(self) -> None:
        if not self._is_closed:
            self._db.flush()
            self._db.close()
            self._is_closed = True

    def _fail_on_closed_store(self) -> None:
        if self._is_closed:
            raise KeyValueStoreError(message="Operation attempted on closed store")

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        self._fail_on_closed_store()

        combo_key: str = compound_key(collection=collection, key=key)

        value: bytes | None = self._db.get(combo_key)

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
        self._fail_on_closed_store()

        combo_key: str = compound_key(collection=collection, key=key)
        json_value: str = managed_entry.to_json()

        self._db[combo_key] = json_value.encode("utf-8")

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        self._fail_on_closed_store()

        combo_key: str = compound_key(collection=collection, key=key)

        # Check if key exists before deleting
        exists = combo_key in self._db

        if exists:
            self._db.delete(combo_key)

        return exists

    def __del__(self) -> None:
        self._close_and_flush()
