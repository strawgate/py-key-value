from datetime import datetime, timezone
from pathlib import Path
from typing import Any, overload

import duckdb
from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

DEFAULT_DUCKDB_STORE_MAX_ENTRIES = 1000


class DuckDBStore(BaseManagedKVStore):
    """A DuckDB-based store that uses SQL operations for key-value storage.
    
    DuckDB is an in-process SQL OLAP database management system that provides 
    excellent performance for analytical queries while supporting standard SQL operations.
    This store can operate in memory-only mode or persist data to disk.
    """

    _connection: duckdb.DuckDBPyConnection
    _max_entries: int | None

    @overload
    def __init__(self, *, connection: duckdb.DuckDBPyConnection, max_entries: int | None = None) -> None: ...

    @overload
    def __init__(self, *, database_path: Path | str | None = None, max_entries: int | None = None) -> None: ...

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
        database_path: Path | str | None = None,
        max_entries: int | None = DEFAULT_DUCKDB_STORE_MAX_ENTRIES,
    ) -> None:
        """Initialize the DuckDB store.

        Args:
            connection: An existing DuckDB connection to use.
            database_path: Path to the database file. If None, uses in-memory database.
                          If ':memory:', explicitly uses in-memory database.
            max_entries: Maximum number of entries to store. When exceeded, oldest entries are removed.
                        If None, no limit is enforced.
        """
        self._max_entries = max_entries
        
        if connection is not None:
            self._connection = connection
        else:
            # Convert Path to string if needed
            if isinstance(database_path, Path):
                database_path = str(database_path)
            
            # Use in-memory database if no path specified
            if database_path is None:
                self._connection = duckdb.connect()
            else:
                self._connection = duckdb.connect(database=database_path)

        super().__init__()

    @override
    async def setup(self) -> None:
        """Initialize the database schema for key-value storage."""
        # Create the main table for storing key-value entries
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS kv_entries (
                collection VARCHAR NOT NULL,
                key VARCHAR NOT NULL,
                value_json TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE,
                ttl DOUBLE,
                expires_at TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (collection, key)
            )
        """)

        # Create index for efficient collection queries
        self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_kv_collection 
            ON kv_entries(collection)
        """)

        # Create index for TTL-based queries (without WHERE clause)
        self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_kv_expires_at 
            ON kv_entries(expires_at)
        """)

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection."""
        await self.setup_collection_once(collection=collection)

        result = self._connection.execute(
            "SELECT value_json FROM kv_entries WHERE collection = ? AND key = ?",
            [collection, key]
        ).fetchone()

        if result is None:
            return None

        value_json = result[0]
        return ManagedEntry.from_json(json_str=value_json)

    @override
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None,
    ) -> None:
        """Store a managed entry by key in the specified collection."""
        await self.setup_collection_once(collection=collection)

        # Use the TTL from the cache_entry if no explicit TTL provided
        effective_ttl = ttl if ttl is not None else cache_entry.ttl

        # Insert or replace the entry
        self._connection.execute("""
            INSERT OR REPLACE INTO kv_entries 
            (collection, key, value_json, created_at, ttl, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            collection,
            key,
            cache_entry.to_json(),
            cache_entry.created_at,
            effective_ttl,
            cache_entry.expires_at
        ])

        # Enforce max entries limit if configured
        await self._enforce_max_entries()

    async def _enforce_max_entries(self) -> None:
        """Remove oldest entries if the store exceeds max_entries limit."""
        if self._max_entries is None:
            return

        # Count current entries
        result = self._connection.execute("SELECT COUNT(*) FROM kv_entries").fetchone()
        current_count = result[0] if result else 0

        if current_count <= self._max_entries:
            return

        # Remove oldest entries (by created_at, then by collection/key for deterministic ordering)
        entries_to_remove = current_count - self._max_entries
        self._connection.execute("""
            DELETE FROM kv_entries 
            WHERE (collection, key) IN (
                SELECT collection, key 
                FROM kv_entries 
                ORDER BY created_at ASC, collection ASC, key ASC 
                LIMIT ?
            )
        """, [entries_to_remove])

    @override
    async def delete(self, collection: str, key: str) -> bool:
        """Delete a key from the specified collection."""
        await self.setup_collection_once(collection=collection)

        result = self._connection.execute(
            "DELETE FROM kv_entries WHERE collection = ? AND key = ? RETURNING key",
            [collection, key]
        )
        
        # Check if any rows were deleted by counting returned rows
        deleted_rows = result.fetchall()
        return len(deleted_rows) > 0

    @override
    async def keys(self, collection: str) -> list[str]:
        """List all non-expired keys in the specified collection."""
        await self.setup_collection_once(collection=collection)

        now = datetime.now(tz=timezone.utc)
        
        result = self._connection.execute("""
            SELECT key FROM kv_entries 
            WHERE collection = ? 
            AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY key
        """, [collection, now]).fetchall()

        return [row[0] for row in result]

    @override
    async def clear_collection(self, collection: str) -> int:
        """Clear all entries in the specified collection."""
        await self.setup_collection_once(collection=collection)

        result = self._connection.execute(
            "DELETE FROM kv_entries WHERE collection = ? RETURNING key",
            [collection]
        )
        
        # Count the number of deleted rows
        deleted_rows = result.fetchall()
        return len(deleted_rows)

    @override
    async def list_collections(self) -> list[str]:
        """List all collections that have at least one non-expired entry."""
        await self.setup_once()

        now = datetime.now(tz=timezone.utc)
        
        result = self._connection.execute("""
            SELECT DISTINCT collection FROM kv_entries 
            WHERE expires_at IS NULL OR expires_at > ?
            ORDER BY collection
        """, [now]).fetchall()

        return [row[0] for row in result]

    @override
    async def cull(self) -> None:
        """Remove all expired entries from the database."""
        await self.setup_once()

        now = datetime.now(tz=timezone.utc)
        
        self._connection.execute(
            "DELETE FROM kv_entries WHERE expires_at IS NOT NULL AND expires_at <= ?",
            [now]
        )