import time
from pathlib import Path
from typing import Any, overload

from key_value.shared.errors import DeserializationError
from key_value.shared.errors.store import KeyValueStoreError
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import BasicSerializationAdapter
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore

try:
    import aiosqlite
except ImportError as e:
    msg = "SQLiteStore requires py-key-value-aio[sqlite]"
    raise ImportError(msg) from e


class SQLiteStore(BaseContextManagerStore, BaseStore):
    """A SQLite-based key-value store using aiosqlite for async operations.

    The store supports two initialization modes:
    - Path-based: Provide a file path, and the store will create and manage the connection.
    - Connection-based: Provide an existing aiosqlite.Connection. The store will NOT close
      externally provided connections - the caller retains ownership and must close it.
    """

    _db: aiosqlite.Connection | None
    _db_path: Path
    _is_closed: bool
    _owns_connection: bool
    _adapter: BasicSerializationAdapter

    @overload
    def __init__(self, *, db: aiosqlite.Connection, default_collection: str | None = None) -> None:
        """Initialize the SQLite store with an existing connection.

        Args:
            db: An existing aiosqlite Connection instance to use. The store will NOT close
                this connection - the caller retains ownership.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, path: Path | str, default_collection: str | None = None) -> None:
        """Initialize the SQLite store with a file path.

        Args:
            path: The path to the SQLite database file. The store will create and manage
                  the connection, closing it when the store is closed.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        db: aiosqlite.Connection | None = None,
        path: Path | str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the SQLite store.

        Args:
            db: An existing aiosqlite Connection instance to use. The store will NOT close
                this connection - the caller retains ownership.
            path: The path to the SQLite database file. The store will create and manage
                  the connection, closing it when the store is closed.
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
            self._db_path = Path(":memory:")
            self._owns_connection = False
        elif path:
            self._db_path = Path(path)
            self._db = None
            self._owns_connection = True

        self._is_closed = False
        self._adapter = BasicSerializationAdapter(date_format=None, value_format="dict")

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Initialize the database connection and create the table if needed."""
        if self._db is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(str(self._db_path))

        # Enable WAL mode for better concurrency
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA synchronous=NORMAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")  # 5 second timeout

        # Create table for key-value storage
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL
            )
            """
        )

        # Create index on expires_at for efficient TTL queries
        await self._db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_expires_at ON kv_store(expires_at)
            """
        )

        await self._db.commit()

    @override
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # pyright: ignore[reportAny]
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self._close()

    @override
    async def _close(self) -> None:
        await self._close_and_flush()

    async def _close_and_flush(self) -> None:
        """Close and flush the database connection if we own it."""
        if not self._is_closed and self._db is not None and self._owns_connection:
            await self._db.commit()
            await self._db.close()
            self._is_closed = True

    def _fail_on_closed_store(self) -> None:
        if self._is_closed:
            raise KeyValueStoreError(message="Operation attempted on closed store")

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        self._fail_on_closed_store()

        if self._db is None:
            raise KeyValueStoreError(message="Database not initialized")

        combo_key: str = compound_key(collection=collection, key=key)

        # Query the value and expiration time
        async with self._db.execute("SELECT value, expires_at FROM kv_store WHERE key = ?", (combo_key,)) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        value_str, expires_at = row

        # Check if the entry has expired
        current_time = time.time()

        if expires_at is not None and current_time > expires_at:
            # Delete expired entry
            await self._db.execute("DELETE FROM kv_store WHERE key = ?", (combo_key,))
            await self._db.commit()
            return None

        # Calculate remaining TTL (clamped to non-negative)
        expires_at_datetime = None
        if expires_at is not None:
            ttl_remaining = max(0.0, expires_at - current_time)
            if ttl_remaining > 0:
                from key_value.shared.utils.time_to_live import now_plus

                expires_at_datetime = now_plus(seconds=ttl_remaining)

        try:
            managed_entry: ManagedEntry = self._adapter.load_json(json_str=value_str)
            # Update expires_at from our TTL tracking
            if expires_at_datetime is not None:
                managed_entry = ManagedEntry(
                    value=managed_entry.value,
                    created_at=managed_entry.created_at,
                    expires_at=expires_at_datetime,
                )
            return managed_entry
        except DeserializationError:
            # If deserialization fails, delete the corrupt entry
            await self._db.execute("DELETE FROM kv_store WHERE key = ?", (combo_key,))
            await self._db.commit()
            return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        self._fail_on_closed_store()

        if self._db is None:
            raise KeyValueStoreError(message="Database not initialized")

        combo_key: str = compound_key(collection=collection, key=key)
        json_value: str = self._adapter.dump_json(entry=managed_entry, exclude_none=True)

        # Calculate expiration time if TTL is set
        expires_at: float | None = None
        if managed_entry.expires_at is not None:
            expires_at = managed_entry.expires_at.timestamp()

        # Insert or replace the entry
        await self._db.execute(
            "INSERT OR REPLACE INTO kv_store (key, value, expires_at) VALUES (?, ?, ?)", (combo_key, json_value, expires_at)
        )
        await self._db.commit()

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        self._fail_on_closed_store()

        if self._db is None:
            raise KeyValueStoreError(message="Database not initialized")

        combo_key: str = compound_key(collection=collection, key=key)

        # Use total_changes to atomically detect if the delete affected any rows
        before = self._db.total_changes
        await self._db.execute("DELETE FROM kv_store WHERE key = ?", (combo_key,))
        await self._db.commit()

        return (self._db.total_changes - before) > 0

    async def __aenter__(self) -> "SQLiteStore":
        await self.setup()
        return self
