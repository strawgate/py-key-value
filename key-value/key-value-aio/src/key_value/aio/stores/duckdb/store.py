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

    The store uses native DuckDB types (JSON, TIMESTAMP) to enable efficient SQL queries
    on stored data. Users can query the database directly for analytics or data exploration.

    Note on connection ownership: When you provide an existing connection, the store
    will take ownership and close it when the store is closed or garbage collected.
    If you need to reuse a connection, create separate DuckDB connections for each store.
    """

    _connection: duckdb.DuckDBPyConnection
    _is_closed: bool
    _owns_connection: bool
    _use_json_column: bool

    @overload
    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        use_json_column: bool = True,
    ) -> None:
        """Initialize the DuckDB store with an existing connection.

        Warning: The store will take ownership of the connection and close it
        when the store is closed or garbage collected. If you need to reuse
        a connection, create separate DuckDB connections for each store.

        Args:
            connection: An existing DuckDB connection to use.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            use_json_column: If True, use native JSON column type; if False, use TEXT.
                Default is True for better queryability and native type support.
        """

    @overload
    def __init__(
        self,
        *,
        database_path: Path | str | None = None,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        use_json_column: bool = True,
    ) -> None:
        """Initialize the DuckDB store with a database path.

        Args:
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            use_json_column: If True, use native JSON column type; if False, use TEXT.
                Default is True for better queryability and native type support.
        """

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
        database_path: Path | str | None = None,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        use_json_column: bool = True,
    ) -> None:
        """Initialize the DuckDB store.

        Args:
            connection: An existing DuckDB connection to use.
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            use_json_column: If True, use native JSON column type; if False, use TEXT.
                Default is True for better queryability and native type support.
        """
        if connection is not None and database_path is not None:
            msg = "Provide only one of connection or database_path"
            raise ValueError(msg)

        if connection is not None:
            self._connection = connection
            self._owns_connection = True  # We take ownership even of provided connections
        else:
            # Convert Path to string if needed
            if isinstance(database_path, Path):
                database_path = str(database_path)

            # Use in-memory database if no path specified
            if database_path is None or database_path == ":memory:":
                self._connection = duckdb.connect(":memory:")
            else:
                self._connection = duckdb.connect(database=database_path)
            self._owns_connection = True

        self._is_closed = False
        self._use_json_column = use_json_column
        self._stable_api = False

        super().__init__(default_collection=default_collection, seed=seed)

    @override
    async def _setup(self) -> None:
        """Initialize the database schema for key-value storage.

        The schema uses native DuckDB types for efficient querying:
        - value: JSON or TEXT column storing the actual value data (not full ManagedEntry)
        - created_at: TIMESTAMP for native datetime operations
        - ttl: DOUBLE for time-to-live in seconds
        - expires_at: TIMESTAMP for native expiration queries

        This design enables:
        - Direct SQL queries on the database for analytics
        - Efficient expiration cleanup: DELETE FROM kv_entries WHERE expires_at < now()
        - Metadata queries without JSON deserialization
        - No data duplication (metadata in columns, value in JSON/TEXT)
        """
        # Determine column type based on use_json_column setting
        value_column_type = "JSON" if self._use_json_column else "TEXT"

        # Create the main table for storing key-value entries
        self._connection.execute(f"""
            CREATE TABLE IF NOT EXISTS kv_entries (
                collection VARCHAR NOT NULL,
                key VARCHAR NOT NULL,
                value {value_column_type} NOT NULL,
                created_at TIMESTAMP,
                ttl DOUBLE,
                expires_at TIMESTAMP,
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
        """Retrieve a managed entry by key from the specified collection.

        Reconstructs the ManagedEntry from the value column and metadata columns.
        The value column contains only the value data (not the full ManagedEntry),
        and metadata (created_at, ttl, expires_at) is stored in separate columns.
        """
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

        result = self._connection.execute(
            "SELECT value, created_at, ttl, expires_at FROM kv_entries WHERE collection = ? AND key = ?",
            [collection, key],
        ).fetchone()

        if result is None:
            return None

        value_data, created_at, ttl, expires_at = result

        # Convert value from JSON/TEXT to dict
        # If it's already a dict (from JSON column), use it; otherwise parse from string
        if isinstance(value_data, str):
            import json
            value = json.loads(value_data)
        else:
            value = value_data

        # DuckDB always returns naive timestamps, but ManagedEntry expects timezone-aware ones
        # Convert to timezone-aware UTC timestamps
        from datetime import timezone

        if created_at is not None and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        if expires_at is not None and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        # Reconstruct ManagedEntry with metadata from columns
        return ManagedEntry(
            value=value,
            created_at=created_at,
            ttl=ttl,
            expires_at=expires_at,
        )

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry by key in the specified collection.

        Stores the value and metadata separately:
        - value: JSON string of just the value data (not full ManagedEntry)
        - created_at, ttl, expires_at: Stored in native columns for efficient querying
        """
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

        # Get just the value as JSON (not the full ManagedEntry)
        value_json = managed_entry.value_as_json

        # Ensure timestamps are timezone-aware (convert naive to UTC if needed)
        from datetime import timezone

        created_at = managed_entry.created_at
        if created_at is not None and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        expires_at = managed_entry.expires_at
        if expires_at is not None and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        # Insert or replace the entry with metadata in separate columns
        self._connection.execute(
            """
            INSERT OR REPLACE INTO kv_entries
            (collection, key, value, created_at, ttl, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            [
                collection,
                key,
                value_json,
                created_at,
                managed_entry.ttl,
                expires_at,
            ],
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry by key from the specified collection."""
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

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
        if not self._is_closed and self._owns_connection:
            self._connection.close()
            self._is_closed = True

    def __del__(self) -> None:
        """Clean up the DuckDB connection on deletion."""
        try:
            if not self._is_closed and self._owns_connection and hasattr(self, "_connection"):
                self._connection.close()
                self._is_closed = True
        except Exception:  # noqa: S110
            # Suppress errors during cleanup to avoid issues during interpreter shutdown
            pass
