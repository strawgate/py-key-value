"""PostgreSQL-based key-value store using asyncpg.

Note: SQL queries in this module use f-strings for table names, which triggers S608 warnings.
This is safe because table names are validated in __init__ to be alphanumeric plus underscores.
"""

# ruff: noqa: S608

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, overload

from key_value.shared.utils.managed_entry import ManagedEntry, dump_to_json, load_from_json
from typing_extensions import Self, override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyCollectionStore, BaseEnumerateCollectionsStore, BaseStore

try:
    import asyncpg
except ImportError as e:
    msg = "PostgreSQLStore requires py-key-value-aio[postgresql]"
    raise ImportError(msg) from e


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_DATABASE = "postgres"
DEFAULT_TABLE = "kv_store"

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

# PostgreSQL table name length limit is 63 characters
POSTGRES_MAX_IDENTIFIER_LEN = 63


class PostgreSQLStore(BaseEnumerateCollectionsStore, BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """PostgreSQL-based key-value store using asyncpg.

    This store uses a single shared table with columns for collection, key, value (JSONB), and metadata.
    Collections are stored as values in the collection column, not as separate tables or SQL identifiers,
    so there are no character restrictions on collection names.

    Example:
        Basic usage with default connection:
        >>> store = PostgreSQLStore()
        >>> async with store:
        ...     await store.put("user_1", {"name": "Alice"}, collection="users")
        ...     user = await store.get("user_1", collection="users")

        Using a connection URL:
        >>> store = PostgreSQLStore(url="postgresql://user:pass@localhost/mydb")
        >>> async with store:
        ...     await store.put("key", {"data": "value"})

        Using custom connection parameters:
        >>> store = PostgreSQLStore(
        ...     host="db.example.com",
        ...     port=5432,
        ...     database="myapp",
        ...     user="myuser",
        ...     password="mypass"
        ... )
    """

    _pool: asyncpg.Pool | None  # type: ignore[type-arg]
    _owns_pool: bool
    _url: str | None
    _host: str
    _port: int
    _database: str
    _user: str | None
    _password: str | None
    _table_name: str

    @overload
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,  # type: ignore[type-arg]
        table_name: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the PostgreSQL store with an existing connection pool.

        Args:
            pool: An existing asyncpg connection pool to use.
            table_name: The name of the table to use for storage (default: kv_store).
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        url: str,
        table_name: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the PostgreSQL store with a connection URL.

        Args:
            url: PostgreSQL connection URL (e.g., postgresql://user:pass@localhost/dbname).
            table_name: The name of the table to use for storage (default: kv_store).
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        database: str = DEFAULT_DATABASE,
        user: str | None = None,
        password: str | None = None,
        table_name: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the PostgreSQL store with connection parameters.

        Args:
            host: PostgreSQL server host (default: localhost).
            port: PostgreSQL server port (default: 5432).
            database: Database name (default: postgres).
            user: Database user (default: current user).
            password: Database password (default: None).
            table_name: The name of the table to use for storage (default: kv_store).
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None = None,  # type: ignore[type-arg]
        url: str | None = None,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        database: str = DEFAULT_DATABASE,
        user: str | None = None,
        password: str | None = None,
        table_name: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the PostgreSQL store."""
        self._pool = pool
        self._owns_pool = pool is None  # Only own the pool if we create it
        self._url = url
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password

        # Validate table name to prevent SQL injection and invalid identifiers
        table_name = table_name or DEFAULT_TABLE
        if not table_name.replace("_", "").isalnum():
            msg = f"Table name must be alphanumeric (with underscores): {table_name}"
            raise ValueError(msg)
        if table_name[0].isdigit():
            msg = f"Table name must not start with a digit: {table_name}"
            raise ValueError(msg)
        # PostgreSQL identifier limit is 63 bytes
        if len(table_name) > POSTGRES_MAX_IDENTIFIER_LEN:
            msg = f"Table name too long (>{POSTGRES_MAX_IDENTIFIER_LEN}): {table_name}"
            raise ValueError(msg)
        self._table_name = table_name

        super().__init__(default_collection=default_collection)

    async def _ensure_pool_initialized(self) -> asyncpg.Pool:  # type: ignore[type-arg]
        """Ensure the connection pool is initialized.

        This method creates the pool lazily if it doesn't exist, allowing the store
        to be used even if the context manager hasn't been entered yet. This is useful
        for frameworks that may call store methods before entering the async context.

        Returns:
            The initialized connection pool.
        """
        if self._pool is None:
            if self._url:
                self._pool = await asyncpg.create_pool(self._url)  # pyright: ignore[reportUnknownMemberType]
            else:
                self._pool = await asyncpg.create_pool(  # pyright: ignore[reportUnknownMemberType]
                    host=self._host,
                    port=self._port,
                    database=self._database,
                    user=self._user,
                    password=self._password,
                )
            self._owns_pool = True
        return self._pool

    @asynccontextmanager
    async def _acquire_connection(self) -> AsyncIterator[asyncpg.Connection]:  # type: ignore[type-arg]
        """Acquire a connection from the pool.

        Yields:
            A connection from the pool.
        """
        pool = await self._ensure_pool_initialized()
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            yield conn

    @override
    async def __aenter__(self) -> Self:
        # Ensure pool is initialized (will be created lazily if needed)
        await self._ensure_pool_initialized()
        await super().__aenter__()
        return self

    @override
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # pyright: ignore[reportAny]
        await super().__aexit__(exc_type, exc_val, exc_tb)
        if self._pool is not None and self._owns_pool:
            await self._pool.close()

    @override
    async def _setup(self) -> None:
        """Set up the database table and indexes if they don't exist.

        This is called once when the store is first used. Since all collections share the same table,
        we only need to set up the schema once.
        """
        # Create the main table if it doesn't exist
        table_sql = (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            "collection VARCHAR(255) NOT NULL, "
            "key VARCHAR(255) NOT NULL, "
            "value JSONB NOT NULL, "
            "ttl DOUBLE PRECISION, "
            "created_at TIMESTAMPTZ, "
            "expires_at TIMESTAMPTZ, "
            "PRIMARY KEY (collection, key))"
        )

        # Create index on expires_at for efficient TTL queries
        # Ensure index name <= 63 chars (PostgreSQL identifier limit)
        index_name = f"idx_{self._table_name}_expires_at"
        if len(index_name) > POSTGRES_MAX_IDENTIFIER_LEN:
            import hashlib

            index_name = "idx_" + hashlib.sha256(self._table_name.encode()).hexdigest()[:16] + "_exp"

        index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL"

        async with self._acquire_connection() as conn:
            await conn.execute(table_sql)  # pyright: ignore[reportUnknownMemberType]
            await conn.execute(index_sql)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection.

        Args:
            key: The key to retrieve.
            collection: The collection to retrieve from.

        Returns:
            The managed entry if found and not expired, None otherwise.
        """
        async with self._acquire_connection() as conn:
            row = await conn.fetchrow(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                f"SELECT value, ttl, created_at, expires_at FROM {self._table_name} WHERE collection = $1 AND key = $2",
                collection,
                key,
            )

            if row is None:
                return None

            # Parse the managed entry - deserialize JSONB value if it's a string
            value_data = row["value"]  # pyright: ignore[reportUnknownVariableType]
            value = load_from_json(value_data) if isinstance(value_data, str) else value_data  # pyright: ignore[reportUnknownVariableType]

            managed_entry = ManagedEntry(
                value=value,  # pyright: ignore[reportUnknownArgumentType]
                created_at=row["created_at"],  # pyright: ignore[reportUnknownArgumentType]
                expires_at=row["expires_at"],  # pyright: ignore[reportUnknownArgumentType]
            )

            # Check if expired and delete if so
            if managed_entry.is_expired:
                await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                    f"DELETE FROM {self._table_name} WHERE collection = $1 AND key = $2",
                    collection,
                    key,
                )
                return None

            return managed_entry

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        """Retrieve multiple managed entries by key from the specified collection.

        Args:
            collection: The collection to retrieve from.
            keys: The keys to retrieve.

        Returns:
            A list of managed entries in the same order as keys, with None for missing/expired entries.
        """
        if not keys:
            return []

        async with self._acquire_connection() as conn:
            # Use ANY to query for multiple keys
            rows = await conn.fetch(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                f"SELECT key, value, ttl, created_at, expires_at FROM {self._table_name} WHERE collection = $1 AND key = ANY($2::text[])",
                collection,
                list(keys),
            )

            # Build a map of key -> managed entry
            entries_by_key: dict[str, ManagedEntry | None] = dict.fromkeys(keys)
            expired_keys: list[str] = []

            for row in rows:  # pyright: ignore[reportUnknownVariableType]
                # Deserialize JSONB value if it's a string
                value_data = row["value"]  # pyright: ignore[reportUnknownVariableType]
                value = load_from_json(value_data) if isinstance(value_data, str) else value_data  # pyright: ignore[reportUnknownVariableType]

                managed_entry = ManagedEntry(
                    value=value,  # pyright: ignore[reportUnknownArgumentType]
                    created_at=row["created_at"],  # pyright: ignore[reportUnknownArgumentType]
                    expires_at=row["expires_at"],  # pyright: ignore[reportUnknownArgumentType]
                )

                if managed_entry.is_expired:
                    expired_keys.append(row["key"])  # pyright: ignore[reportUnknownArgumentType]
                    entries_by_key[row["key"]] = None
                else:
                    entries_by_key[row["key"]] = managed_entry

            # Delete expired entries in batch
            if expired_keys:
                await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                    f"DELETE FROM {self._table_name} WHERE collection = $1 AND key = ANY($2::text[])",
                    collection,
                    expired_keys,
                )

            return [entries_by_key[key] for key in keys]

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry by key in the specified collection.

        Args:
            key: The key to store.
            collection: The collection to store in.
            managed_entry: The managed entry to store.
        """

        async with self._acquire_connection() as conn:
            # Serialize value to JSON string for JSONB column
            value_json = dump_to_json(managed_entry.value_as_dict)

            upsert_sql = (
                f"INSERT INTO {self._table_name} "
                "(collection, key, value, ttl, created_at, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (collection, key) "
                "DO UPDATE SET value = EXCLUDED.value, ttl = EXCLUDED.ttl, expires_at = EXCLUDED.expires_at"
            )
            await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                upsert_sql,
                collection,
                key,
                value_json,
                managed_entry.ttl,
                managed_entry.created_at,
                managed_entry.expires_at,
            )

    @override
    async def _put_managed_entries(
        self,
        *,
        collection: str,
        keys: Sequence[str],
        managed_entries: Sequence[ManagedEntry],
        ttl: float | None,
        created_at: datetime,
        expires_at: datetime | None,
    ) -> None:
        """Store multiple managed entries by key in the specified collection.

        Args:
            collection: The collection to store in.
            keys: The keys to store.
            managed_entries: The managed entries to store.
            ttl: The TTL in seconds (None for no expiration).
            created_at: The creation timestamp for all entries.
            expires_at: The expiration timestamp for all entries (None if no TTL).
        """
        if not keys:
            return

        # Prepare data for batch insert using method-level ttl/created_at/expires_at
        # Serialize each value to JSON string for JSONB column
        values = [
            (collection, key, dump_to_json(entry.value_as_dict), ttl, created_at, expires_at)
            for key, entry in zip(keys, managed_entries, strict=True)
        ]

        async with self._acquire_connection() as conn:
            # Use executemany for batch insert
            batch_upsert_sql = (
                f"INSERT INTO {self._table_name} "
                "(collection, key, value, ttl, created_at, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (collection, key) "
                "DO UPDATE SET value = EXCLUDED.value, ttl = EXCLUDED.ttl, expires_at = EXCLUDED.expires_at"
            )
            await conn.executemany(  # pyright: ignore[reportUnknownMemberType]
                batch_upsert_sql,
                values,
            )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry by key from the specified collection.

        Args:
            key: The key to delete.
            collection: The collection to delete from.

        Returns:
            True if the entry was deleted, False if it didn't exist.
        """
        async with self._acquire_connection() as conn:
            result = await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                f"DELETE FROM {self._table_name} WHERE collection = $1 AND key = $2",
                collection,
                key,
            )
            # PostgreSQL execute returns a string like "DELETE N" where N is the number of rows deleted
            return result.split()[-1] != "0"

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        """Delete multiple managed entries by key from the specified collection.

        Args:
            keys: The keys to delete.
            collection: The collection to delete from.

        Returns:
            The number of entries that were deleted.
        """
        if not keys:
            return 0

        async with self._acquire_connection() as conn:
            result = await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                f"DELETE FROM {self._table_name} WHERE collection = $1 AND key = ANY($2::text[])",
                collection,
                list(keys),
            )
            # PostgreSQL execute returns a string like "DELETE N" where N is the number of rows deleted
            return int(result.split()[-1])

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """List all collection names.

        Args:
            limit: Maximum number of collection names to return.

        Returns:
            A list of collection names.
        """
        if limit is None or limit <= 0:
            limit = DEFAULT_PAGE_SIZE
        limit = min(limit, PAGE_LIMIT)

        async with self._acquire_connection() as conn:
            rows = await conn.fetch(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                f"SELECT DISTINCT collection FROM {self._table_name} ORDER BY collection LIMIT $1",
                limit,
            )

            return [row["collection"] for row in rows]  # pyright: ignore[reportUnknownVariableType]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete all entries in a collection.

        Args:
            collection: The collection to delete.

        Returns:
            True if any entries were deleted, False otherwise.
        """
        async with self._acquire_connection() as conn:
            result = await conn.execute(  # pyright: ignore[reportUnknownMemberType]
                f"DELETE FROM {self._table_name} WHERE collection = $1",
                collection,
            )
            # Return True if any rows were deleted
            return result.split()[-1] != "0"

    @override
    async def _close(self) -> None:
        """Close the connection pool."""
        # Connection pool is closed in __aexit__
