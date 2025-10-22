from pathlib import Path
from typing import Any, overload

from key_value.shared.errors.store import KeyValueStoreError
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore

try:
    import aiosqlite
except ImportError as e:
    msg = "SQLiteStore requires py-key-value-aio[sqlite]"
    raise ImportError(msg) from e


class SQLiteStore(BaseContextManagerStore, BaseStore):
    """A SQLite-based key-value store using aiosqlite for async operations."""

    _db: aiosqlite.Connection | None
    _db_path: Path
    _is_closed: bool

    @overload
    def __init__(self, *, db: aiosqlite.Connection, default_collection: str | None = None) -> None:
        """Initialize the SQLite store.

        Args:
            db: An existing aiosqlite Connection instance to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(self, *, path: Path | str, default_collection: str | None = None) -> None:
        """Initialize the SQLite store.

        Args:
            path: The path to the SQLite database file.
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
            db: An existing aiosqlite Connection instance to use.
            path: The path to the SQLite database file.
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
        elif path:
            self._db_path = Path(path)
            self._db = None

        self._is_closed = False

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Initialize the database connection and create the table if needed."""
        if self._db is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(str(self._db_path))

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
        if not self._is_closed and self._db is not None:
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
        import time

        current_time = time.time()

        if expires_at is not None and current_time > expires_at:
            # Delete expired entry
            await self._db.execute("DELETE FROM kv_store WHERE key = ?", (combo_key,))
            await self._db.commit()
            return None

        # Calculate remaining TTL
        ttl: float | None = None
        if expires_at is not None:
            ttl = expires_at - current_time

        managed_entry: ManagedEntry = ManagedEntry.from_json(json_str=value_str, ttl=ttl)

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

        if self._db is None:
            raise KeyValueStoreError(message="Database not initialized")

        combo_key: str = compound_key(collection=collection, key=key)
        json_value: str = managed_entry.to_json(include_expiration=False)

        # Calculate expiration time if TTL is set
        expires_at: float | None = None
        if managed_entry.ttl is not None:
            import time

            expires_at = time.time() + float(managed_entry.ttl)

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

        # Check if key exists before deleting
        async with self._db.execute("SELECT 1 FROM kv_store WHERE key = ?", (combo_key,)) as cursor:
            exists = await cursor.fetchone() is not None

        if exists:
            await self._db.execute("DELETE FROM kv_store WHERE key = ?", (combo_key,))
            await self._db.commit()

        return exists

    async def __aenter__(self) -> "SQLiteStore":
        await self.setup()
        return self

    def __del__(self) -> None:
        # Note: We can't use async in __del__, so we just mark as closed
        # The connection will be cleaned up by the garbage collector
        self._is_closed = True
