from contextlib import suppress
from typing import overload

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import ALPHANUMERIC_CHARACTERS, sanitize_string
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyCollectionStore, BaseEnumerateCollectionsStore, BaseStore

try:
    from cassandra.cluster import Cluster, Session
except ImportError as e:
    msg = "CassandraStore requires py-key-value-aio[cassandra]"
    raise ImportError(msg) from e


DEFAULT_KEYSPACE = "kv_store"
DEFAULT_TABLE_PREFIX = "kv"

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

# Cassandra table name length limit (48 chars)
MAX_TABLE_LENGTH = 48
TABLE_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_"


class CassandraStore(BaseEnumerateCollectionsStore, BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """Cassandra-based key-value store."""

    _cluster: Cluster
    _session: Session
    _keyspace: str
    _table_prefix: str

    @overload
    def __init__(
        self,
        *,
        cluster: Cluster,
        keyspace: str | None = None,
        table_prefix: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Cassandra store.

        Args:
            cluster: The Cassandra cluster to use.
            keyspace: The name of the Cassandra keyspace.
            table_prefix: The prefix for Cassandra tables.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        contact_points: list[str] | None = None,
        port: int = 9042,
        keyspace: str | None = None,
        table_prefix: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Cassandra store.

        Args:
            contact_points: The contact points of the Cassandra cluster.
            port: The port of the Cassandra cluster.
            keyspace: The name of the Cassandra keyspace.
            table_prefix: The prefix for Cassandra tables.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        cluster: Cluster | None = None,
        contact_points: list[str] | None = None,
        port: int = 9042,
        keyspace: str | None = None,
        table_prefix: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Cassandra store."""

        if cluster:
            self._cluster = cluster
        else:
            contact_points = contact_points or ["127.0.0.1"]
            self._cluster = Cluster(contact_points=contact_points, port=port)

        self._keyspace = keyspace or DEFAULT_KEYSPACE
        self._table_prefix = table_prefix or DEFAULT_TABLE_PREFIX
        self._session = self._cluster.connect()  # pyright: ignore[reportUnknownMemberType]

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        # Create keyspace if it doesn't exist
        self._session.execute(  # pyright: ignore[reportUnknownMemberType]
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        self._session.set_keyspace(self._keyspace)  # pyright: ignore[reportUnknownMemberType]

    def _sanitize_table_name(self, collection: str) -> str:
        sanitized = sanitize_string(
            value=collection, max_length=MAX_TABLE_LENGTH - len(self._table_prefix) - 1, allowed_characters=TABLE_ALLOWED_CHARACTERS
        )
        return f"{self._table_prefix}_{sanitized}"

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        table_name = self._sanitize_table_name(collection=collection)

        # Create table if it doesn't exist
        self._session.execute(  # pyright: ignore[reportUnknownMemberType]
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                key text PRIMARY KEY,
                value text,
                created_at timestamp,
                expires_at timestamp
            )
            """
        )

        # Create index on expires_at for TTL queries
        with suppress(Exception):
            # Index might already exist, ignore errors
            self._session.execute(f"CREATE INDEX IF NOT EXISTS ON {table_name} (expires_at)")  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        table_name = self._sanitize_table_name(collection=collection)

        query = f"SELECT value FROM {table_name} WHERE key = %s"  # noqa: S608 - table_name is sanitized
        result = self._session.execute(query, (key,))  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        row = result.one()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if not row:
            return None

        json_value: str | None = row.value

        if not isinstance(json_value, str):
            return None

        return ManagedEntry.from_json(json_str=json_value)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        json_value: str = managed_entry.to_json()
        table_name = self._sanitize_table_name(collection=collection)

        # table_name is sanitized
        query = f"""
            INSERT INTO {table_name} (key, value, created_at, expires_at)
            VALUES (%s, %s, %s, %s)
        """  # noqa: S608

        self._session.execute(  # pyright: ignore[reportUnknownMemberType]
            query,
            (
                key,
                json_value,
                managed_entry.created_at if managed_entry.created_at else None,
                managed_entry.expires_at if managed_entry.expires_at else None,
            ),
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        table_name = self._sanitize_table_name(collection=collection)

        # Check if key exists first
        check_query = f"SELECT key FROM {table_name} WHERE key = %s"  # noqa: S608 - table_name is sanitized
        result = self._session.execute(check_query, (key,))  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        exists = result.one() is not None  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if exists:
            delete_query = f"DELETE FROM {table_name} WHERE key = %s"  # noqa: S608 - table_name is sanitized
            self._session.execute(delete_query, (key,))  # pyright: ignore[reportUnknownMemberType]

        return exists

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        # Query system tables for table names
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = self._session.execute(query, (self._keyspace,))  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        collections = []
        prefix_len = len(self._table_prefix) + 1

        for row in result:  # pyright: ignore[reportUnknownVariableType]
            table_name = row.table_name  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if table_name.startswith(f"{self._table_prefix}_"):
                collection_name = table_name[prefix_len:]
                collections.append(collection_name)

            if len(collections) >= limit:
                break

        return collections

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        table_name = self._sanitize_table_name(collection=collection)

        # Check if table exists
        check_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s"
        result = self._session.execute(check_query, (self._keyspace, table_name))  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        exists = result.one() is not None  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if exists:
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            self._session.execute(drop_query)  # pyright: ignore[reportUnknownMemberType]

        return exists

    @override
    async def _close(self) -> None:
        self._cluster.shutdown()
