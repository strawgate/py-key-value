from pathlib import Path
from typing import overload

from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import SEED_DATA_TYPE, BaseContextManagerStore, BaseStore

try:
    import duckdb
except ImportError as e:
    msg = "DuckDBStore requires py-key-value-aio[duckdb]"
    raise ImportError(msg) from e


class DuckDBStore(BaseContextManagerStore, BaseStore):
    """A DuckDB-based key-value store supporting both in-memory and persistent storage.

    DuckDB is an in-process SQL OLAP database that provides excellent performance
    for analytical workloads while supporting standard SQL operations. This store
    can operate in memory-only mode or persist data to disk.
    """

    _connection: duckdb.DuckDBPyConnection
    _is_closed: bool

    @overload
    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store with an existing connection.

        Args:
            connection: An existing DuckDB connection to use.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
        """

    @overload
    def __init__(
        self,
        *,
        database_path: Path | str | None = None,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store with a database path.

        Args:
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
        """

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
        database_path: Path | str | None = None,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store.

        Args:
            connection: An existing DuckDB connection to use.
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
        """
        if connection is not None and database_path is not None:
            msg = "Provide only one of connection or database_path"
            raise ValueError(msg)

        if connection is not None:
            self._connection = connection
        else:
            # Convert Path to string if needed
            if isinstance(database_path, Path):
                database_path = str(database_path)

            # Use in-memory database if no path specified
            if database_path is None or database_path == ":memory:":
                self._connection = duckdb.connect(":memory:")
            else:
                self._connection = duckdb.connect(database=database_path)

        self._is_closed = False
        self._stable_api = False

        super().__init__(default_collection=default_collection, seed=seed)

    @override
    async def _setup(self) -> None:
        """Initialize the database schema for key-value storage."""
        # Create the main table for storing key-value entries
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS kv_entries (
                collection VARCHAR NOT NULL,
                key VARCHAR NOT NULL,
                value_json TEXT NOT NULL,
                created_at DOUBLE,
                ttl DOUBLE,
                expires_at DOUBLE,
                PRIMARY KEY (collection, key)
            )
        """)

        # Create index for efficient collection queries
        self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_kv_collection
            ON kv_entries(collection)
        """)

        # Create index for expiration-based queries
        self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_kv_expires_at
            ON kv_entries(expires_at)
        """)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection."""
        result = self._connection.execute(
            "SELECT value_json FROM kv_entries WHERE collection = ? AND key = ?",
            [collection, key],
        ).fetchone()

        if result is None:
            return None

        value_json = result[0]
        return ManagedEntry.from_json(json_str=value_json)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry by key in the specified collection."""
        # Insert or replace the entry
        self._connection.execute(
            """
            INSERT OR REPLACE INTO kv_entries
            (collection, key, value_json, created_at, ttl, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            [
                collection,
                key,
                managed_entry.to_json(),
                managed_entry.created_at.timestamp() if managed_entry.created_at else None,
                managed_entry.ttl,
                managed_entry.expires_at.timestamp() if managed_entry.expires_at else None,
            ],
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry by key from the specified collection."""
        result = self._connection.execute(
            "DELETE FROM kv_entries WHERE collection = ? AND key = ? RETURNING key",
            [collection, key],
        )

        # Check if any rows were deleted by counting returned rows
        deleted_rows = result.fetchall()
        return len(deleted_rows) > 0

    @override
    async def _close(self) -> None:
        """Close the DuckDB connection."""
        if not self._is_closed:
            self._connection.close()
            self._is_closed = True

    def __del__(self) -> None:
        """Clean up the DuckDB connection on deletion."""
        if not self._is_closed:
            self._connection.close()
            self._is_closed = True
