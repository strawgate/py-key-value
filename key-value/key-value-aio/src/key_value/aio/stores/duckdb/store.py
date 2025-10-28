from pathlib import Path
from typing import Any, cast, overload

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

    Storage modes:
    - native_storage=True: Stores values in a JSON column as native dicts for queryability
    - native_storage=False: Stores values as stringified JSON in a TEXT column

    Note on connection ownership: When you provide an existing connection, the store
    will take ownership and close it when the store is closed or garbage collected.
    If you need to reuse a connection, create separate DuckDB connections for each store.
    """

    _connection: duckdb.DuckDBPyConnection
    _is_closed: bool
    _owns_connection: bool
    _native_storage: bool
    _table_name: str

    @overload
    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        table_name: str = "kv_entries",
        native_storage: bool = True,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store with an existing connection.

        Warning: The store will take ownership of the connection and close it
        when the store is closed or garbage collected. If you need to reuse
        a connection, create separate DuckDB connections for each store.

        Args:
            connection: An existing DuckDB connection to use.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            native_storage: If True, use native JSON column for dict storage; if False, use TEXT for stringified JSON.
                Default is True for better queryability and native type support.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
        """

    @overload
    def __init__(
        self,
        *,
        database_path: Path | str | None = None,
        table_name: str = "kv_entries",
        native_storage: bool = True,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store with a database path.

        Args:
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            native_storage: If True, use native JSON column for dict storage; if False, use TEXT for stringified JSON.
                Default is True for better queryability and native type support.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
        """

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
        database_path: Path | str | None = None,
        table_name: str = "kv_entries",
        native_storage: bool = True,
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
    ) -> None:
        """Initialize the DuckDB store.

        Args:
            connection: An existing DuckDB connection to use.
            database_path: Path to the database file. If None or ':memory:', uses in-memory database.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            native_storage: If True, use native JSON column for dict storage; if False, use TEXT for stringified JSON.
                Default is True for better queryability and native type support.
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
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
        self._native_storage = native_storage
        self._table_name = table_name
        self._stable_api = False

        super().__init__(default_collection=default_collection, seed=seed)

    def _get_create_table_sql(self) -> str:
        """Generate SQL for creating the key-value entries table.

        Returns:
            SQL CREATE TABLE statement.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                collection VARCHAR NOT NULL,
                key VARCHAR NOT NULL,
                value_json TEXT,
                value_dict JSON,
                created_at TIMESTAMP,
                ttl DOUBLE,
                expires_at TIMESTAMP,
                PRIMARY KEY (collection, key)
            )
        """

    def _get_create_collection_index_sql(self) -> str:
        """Generate SQL for creating index on collection column.

        Returns:
            SQL CREATE INDEX statement.
        """
        return f"""
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_collection
            ON {self._table_name}(collection)
        """

    def _get_create_expires_index_sql(self) -> str:
        """Generate SQL for creating index on expires_at column.

        Returns:
            SQL CREATE INDEX statement.
        """
        return f"""
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at)
        """

    def _get_select_sql(self) -> str:
        """Generate SQL for selecting an entry by collection and key.

        Returns:
            SQL SELECT statement with placeholders.
        """
        return f"""
            SELECT value_json, value_dict, created_at, ttl, expires_at
            FROM {self._table_name}
            WHERE collection = ? AND key = ?
        """  # noqa: S608

    def _get_insert_sql(self) -> str:
        """Generate SQL for inserting or replacing an entry.

        Returns:
            SQL INSERT OR REPLACE statement with placeholders.
        """
        return f"""
            INSERT OR REPLACE INTO {self._table_name}
            (collection, key, value_json, value_dict, created_at, ttl, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """  # noqa: S608

    def _get_delete_sql(self) -> str:
        """Generate SQL for deleting an entry by collection and key.

        Returns:
            SQL DELETE statement with RETURNING clause.
        """
        return f"""
            DELETE FROM {self._table_name}
            WHERE collection = ? AND key = ?
            RETURNING key
        """  # noqa: S608

    @override
    async def _setup(self) -> None:
        """Initialize the database schema for key-value storage.

        The schema uses native DuckDB types for efficient querying:
        - value_json: TEXT column storing stringified JSON (used when native_storage=False)
        - value_dict: JSON column storing native dicts (used when native_storage=True)
        - created_at: TIMESTAMP for native datetime operations
        - ttl: DOUBLE for time-to-live in seconds
        - expires_at: TIMESTAMP for native expiration queries

        This design follows the Elasticsearch/MongoDB pattern of separating native and stringified
        storage, enabling:
        - Direct SQL queries on the database for analytics (when using native storage)
        - Efficient expiration cleanup: DELETE FROM table WHERE expires_at < now()
        - Metadata queries without JSON deserialization
        - Flexibility to choose between native dict storage and stringified JSON
        """
        # Create the main table for storing key-value entries
        self._connection.execute(self._get_create_table_sql())

        # Create index for efficient collection queries
        self._connection.execute(self._get_create_collection_index_sql())

        # Create index for expiration-based queries
        self._connection.execute(self._get_create_expires_index_sql())

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection.

        Reconstructs the ManagedEntry from value columns and metadata columns.
        Tries value_dict first (native storage), falls back to value_json (stringified).
        """
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

        result = self._connection.execute(
            self._get_select_sql(),
            [collection, key],
        ).fetchone()

        if result is None:
            return None

        value_json, value_dict, created_at, ttl, expires_at = result

        # Determine which value column to use (prefer value_dict if present)
        import json

        value: dict[str, Any]
        if value_dict is not None:
            # Native storage mode - value_dict can be dict or string (DuckDB JSON returns as string)
            if isinstance(value_dict, dict):
                value = cast(dict[str, Any], value_dict)
            elif isinstance(value_dict, str):
                # DuckDB sometimes returns JSON as string
                value = json.loads(value_dict)
            else:
                msg = f"value_dict has unexpected type: {type(value_dict)}"
                raise TypeError(msg)
        elif value_json is not None:
            # Stringified JSON mode - parse from string
            value = json.loads(value_json)
        else:
            # Neither column has data - this shouldn't happen
            return None

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
        - value_json/value_dict: Stores value based on native_storage setting
        - created_at, ttl, expires_at: Stored in native columns for efficient querying
        """
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

        # Store in appropriate column based on native_storage setting
        value_json: str | None = None
        value_dict: str | None = None

        if self._native_storage:
            # Native storage: store as JSON string in JSON column (DuckDB will handle as JSON type)
            # We use value_as_json to ensure serialization errors are caught
            value_dict = managed_entry.value_as_json
        else:
            # Stringified storage: store JSON string in TEXT column
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
            self._get_insert_sql(),
            [
                collection,
                key,
                value_json,
                value_dict,
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
            self._get_delete_sql(),
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
