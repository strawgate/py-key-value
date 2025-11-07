import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, overload

from key_value.shared.errors import DeserializationError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.serialization import SerializationAdapter
from typing_extensions import override

from key_value.aio.stores.base import SEED_DATA_TYPE, BaseContextManagerStore, BaseStore

try:
    import duckdb
except ImportError as e:
    msg = "DuckDBStore requires py-key-value-aio[duckdb]"
    raise ImportError(msg) from e


class DuckDBSerializationAdapter(SerializationAdapter):
    """Adapter for DuckDB with support for native JSON and TEXT storage modes."""

    _native_storage: bool

    def __init__(self, *, native_storage: bool = True) -> None:
        """Initialize the DuckDB adapter.

        Args:
            native_storage: If True, use JSON column for native dict storage.
                          If False, use TEXT column for stringified JSON.
        """
        super().__init__()

        self._native_storage = native_storage
        self._date_format = "datetime"
        # Always use string format - DuckDB needs JSON strings for both TEXT and JSON columns
        self._value_format = "string"

    @override
    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare data for dumping to DuckDB.

        Moves the value to the appropriate column (value_dict or value_json)
        and sets the other column to None. Also includes version, key, and collection
        fields in the JSON for compatibility with deserialization.
        """
        value = data.pop("value")

        # Extract version, key, and collection to include in the JSON
        version = data.pop("version", None)
        key = data.pop("key", None)
        collection_name = data.pop("collection", None)

        # Build the document to store in JSON columns
        json_document: dict[str, Any] = {"value": value}

        if version is not None:
            json_document["version"] = version
        if key is not None:
            json_document["key"] = key
        if collection_name is not None:
            json_document["collection"] = collection_name

        # Set both columns to None, then populate the appropriate one
        data["value_json"] = None
        data["value_dict"] = None

        if self._native_storage:
            # For native storage, convert the document to JSON string for DuckDB's JSON column
            # DuckDB will parse it and store it as native JSON
            data["value_dict"] = json.dumps(json_document)
        else:
            # For TEXT storage, store as JSON string
            data["value_json"] = json.dumps(json_document)

        return data

    @override
    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare data loaded from DuckDB for conversion to ManagedEntry.

        Extracts value, version, key, and collection from the JSON columns
        and handles timezone conversion for DuckDB's naive timestamps.
        """
        value_json = data.pop("value_json", None)
        value_dict = data.pop("value_dict", None)

        # Parse the JSON document from the appropriate column
        json_document = self._parse_json_column(value_dict, value_json)

        # Extract fields from the JSON document
        data["value"] = json_document.get("value")
        if "version" in json_document:
            data["version"] = json_document["version"]
        if "key" in json_document:
            data["key"] = json_document["key"]
        if "collection" in json_document:
            data["collection"] = json_document["collection"]

        # DuckDB always returns naive timestamps, but ManagedEntry expects timezone-aware ones
        self._convert_timestamps_to_utc(data)

        return data

    def _parse_json_column(self, value_dict: Any, value_json: Any) -> dict[str, Any]:  # noqa: ANN401
        """Parse JSON from value_dict or value_json column."""
        if value_dict is not None:
            # Native storage mode - value_dict can be dict or string (DuckDB JSON returns as string)
            if isinstance(value_dict, dict):
                return cast(dict[str, Any], value_dict)
            if isinstance(value_dict, str):
                parsed: dict[str, Any] = json.loads(value_dict)
                return parsed
            msg = f"value_dict has unexpected type: {type(value_dict)}"
            raise DeserializationError(message=msg)

        if value_json is not None:
            # Stringified JSON mode - parse from string
            if isinstance(value_json, str):
                parsed_json: dict[str, Any] = json.loads(value_json)
                return parsed_json
            msg = f"value_json has unexpected type: {type(value_json)}"
            raise DeserializationError(message=msg)

        msg = "Neither value_dict nor value_json column contains data"
        raise DeserializationError(message=msg)

    def _convert_timestamps_to_utc(self, data: dict[str, Any]) -> None:
        """Convert naive timestamps to UTC timezone-aware timestamps."""
        created_at = data.get("created_at")
        if created_at is not None and isinstance(created_at, datetime) and created_at.tzinfo is None:
            data["created_at"] = created_at.astimezone(tz=timezone.utc)

        expires_at = data.get("expires_at")
        if expires_at is not None and isinstance(expires_at, datetime) and expires_at.tzinfo is None:
            data["expires_at"] = expires_at.astimezone(tz=timezone.utc)


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
    _adapter: SerializationAdapter
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
        self._adapter = DuckDBSerializationAdapter(native_storage=native_storage)
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
            SELECT value_json, value_dict, created_at, expires_at
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
            (collection, key, value_json, value_dict, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
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

        Reconstructs the ManagedEntry from value columns and metadata columns
        using the serialization adapter.
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

        value_json, value_dict, created_at, expires_at = result

        # Build document dict for the adapter (exclude None values)
        document: dict[str, Any] = {
            "value_json": value_json,
            "value_dict": value_dict,
        }

        if created_at is not None and isinstance(created_at, datetime):
            document["created_at"] = created_at.astimezone(tz=timezone.utc)
        if expires_at is not None and isinstance(expires_at, datetime):
            document["expires_at"] = expires_at.astimezone(tz=timezone.utc)

        try:
            return self._adapter.load_dict(data=document)
        except DeserializationError:
            return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry by key in the specified collection.

        Uses the serialization adapter to convert the ManagedEntry to the
        appropriate storage format.
        """
        if self._is_closed:
            msg = "Cannot operate on closed DuckDBStore"
            raise RuntimeError(msg)

        # Use adapter to dump the managed entry to a dict with key and collection
        document = self._adapter.dump_dict(entry=managed_entry, exclude_none=False, key=key, collection=collection)

        # Insert or replace the entry with metadata in separate columns
        self._connection.execute(
            self._get_insert_sql(),
            [
                collection,
                key,
                document["value_json"],
                document["value_dict"],
                document.get("created_at"),
                document.get("expires_at"),
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
