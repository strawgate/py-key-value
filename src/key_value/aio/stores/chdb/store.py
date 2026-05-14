"""chDB (embedded ClickHouse) key-value store."""

import json
import re
from pathlib import Path
from typing import Any, overload

from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio._utils.serialization import BasicSerializationAdapter
from key_value.aio.errors import DeserializationError
from key_value.aio.stores.base import SEED_DATA_TYPE, BaseContextManagerStore, BaseStore

try:
    from chdb.session import Session
except ImportError as e:
    msg = "ChDBStore requires py-key-value-aio[chdb]"
    raise ImportError(msg) from e


# chDB performs ClickHouse string-literal escape parsing on substituted
# ``{name:String}`` parameters, so we have to escape any character that would
# otherwise terminate or alter the literal (backslash, quote, control bytes).
_CHDB_STRING_ESCAPES = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        "\x00": "\\0",
        "\x08": "\\b",
        "\t": "\\t",
        "\n": "\\n",
        "\x0b": "\\v",
        "\x0c": "\\f",
        "\r": "\\r",
    }
)


def _escape_string_param(value: str) -> str:
    """Escape a string before substitution into a chDB parameterized query."""
    return value.translate(_CHDB_STRING_ESCAPES)


class ChDBStore(BaseContextManagerStore, BaseStore):
    """A chDB-based key-value store backed by an embedded ClickHouse engine.

    chDB (https://clickhouse.com/chdb) embeds ClickHouse in-process, providing
    a SQL OLAP engine with optional persistence. This store can operate in
    memory-only mode or persist data to disk.

    Important: chDB uses a **process-global** embedded ClickHouse server. Only
    one ``database_path`` can be active per process at a time. Multiple sessions
    with the *same* path (or multiple ``:memory:`` sessions) are fine, but
    attempting to open a session with a different path while another is open
    will raise ``RuntimeError``. Close all sessions/stores for the previous path
    before switching.

    Entries are stored in a ``ReplacingMergeTree`` table keyed by
    ``(collection, key)``. The latest insert wins on read (via the ``FINAL``
    modifier) and old versions are reclaimed during background merges; calling
    ``OPTIMIZE TABLE ... FINAL`` will compact eagerly.

    JSON-encoded values, ISO-8601 timestamp strings, and the document schema
    version are stored in plain ``String`` columns to keep parameterized
    inserts and reads simple.

    Note: The chDB library is synchronous, so operations block the running
    event loop for the duration of each query. For latency-sensitive async
    applications, consider wrapping calls with ``asyncio.to_thread`` or using
    a store backed by a natively async driver.

    Note: ClickHouse's lightweight ``DELETE`` does not report affected row
    counts, so :py:meth:`delete` performs an existence check followed by the
    delete and returns the existence result. The boolean is best-effort and
    not strictly atomic with the actual removal.

    Note: Expired entries (those past their ``expires_at`` timestamp) are
    filtered on read but are **not** automatically culled from storage.
    Applications that set TTLs on many entries should periodically run::

        DELETE FROM <table> WHERE expires_at != '' AND expires_at < now()

    to reclaim space.
    """

    _session: Session
    _table_name: str
    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        session: Session,
        table_name: str = "kv_entries",
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the chDB store with an existing session.

        Note: If you provide a session, the store will NOT manage its lifecycle (will not
        close it). The caller is responsible for managing the session's lifecycle.

        Args:
            session: An existing chDB session to use.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            auto_create: Whether to automatically create the table if it doesn't exist. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        database_path: Path | str | None = None,
        table_name: str = "kv_entries",
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the chDB store with a database path.

        Args:
            database_path: Path to the database directory. If None or ':memory:', uses in-memory
                database. Note: chDB uses a process-global embedded server — only one path can
                be active at a time. All stores/sessions for a previous path must be closed
                before opening a different path.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            auto_create: Whether to automatically create the table if it doesn't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        session: Session | None = None,
        database_path: Path | str | None = None,
        table_name: str = "kv_entries",
        default_collection: str | None = None,
        seed: SEED_DATA_TYPE | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the chDB store.

        Args:
            session: An existing chDB session to use. If provided, the store will NOT
                manage its lifecycle (will not close it). The caller is responsible for managing
                the session's lifecycle.
            database_path: Path to the database directory. If None or ':memory:', uses in-memory
                database. Note: chDB uses a process-global embedded server — only one path can
                be active at a time. All stores/sessions for a previous path must be closed
                before opening a different path.
            table_name: Name of the table to store key-value entries. Defaults to "kv_entries".
            default_collection: The default collection to use if no collection is provided.
            seed: Optional seed data to pre-populate the store.
            auto_create: Whether to automatically create the table if it doesn't exist. Defaults to True.
                When False, raises ValueError if the table doesn't exist.
        """
        if session is not None and database_path is not None:
            msg = "Provide only one of session or database_path"
            raise ValueError(msg)

        client_provided = session is not None

        if session is not None:
            self._session = session
        else:
            if isinstance(database_path, Path):
                database_path = str(database_path)

            if database_path is None or database_path == ":memory:":
                self._session = Session(":memory:")
            else:
                self._session = Session(database_path)

        # Values are stored as JSON strings and dates as ISO-8601 strings in
        # ``String`` columns. Empty strings round-trip back to missing values
        # via the adapter's truthy-check on load.
        adapter = BasicSerializationAdapter(date_format="isoformat", value_format="string")

        # ClickHouse itself accepts any identifier when wrapped in backticks
        # (digit-leading names, hyphens, unicode, etc.), but we embed the
        # table name unquoted into SQL strings, so restrict it to a safe
        # bare-identifier subset to avoid SQL injection. Quoting + backtick
        # escaping would lift this restriction if needed later.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            msg = "Table name must start with a letter or underscore and contain only letters, digits, or underscores"
            raise ValueError(msg)
        self._table_name = table_name
        self._auto_create = auto_create

        super().__init__(
            default_collection=default_collection,
            seed=seed,
            client_provided_by_user=client_provided,
            stable_api=False,
            serialization_adapter=adapter,
        )

    def _get_create_table_sql(self) -> str:
        """SQL for creating the key-value entries table.

        ``inserted_at`` (auto-populated via ``DEFAULT now64(9)``) is used as
        the version column for ``ReplacingMergeTree`` so that the most recent
        write wins on read.
        """
        return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                collection String,
                key String,
                value String,
                created_at String,
                expires_at String,
                version Int32,
                inserted_at DateTime64(9, 'UTC') DEFAULT now64(9)
            ) ENGINE = ReplacingMergeTree(inserted_at)
            ORDER BY (collection, key)
        """

    def _get_select_sql(self) -> str:
        """SQL for selecting the latest entry by ``(collection, key)``."""
        return f"""
            SELECT value, created_at, expires_at, version
            FROM {self._table_name} FINAL
            WHERE collection = {{collection:String}} AND key = {{key:String}}
            LIMIT 1
        """  # noqa: S608

    def _get_insert_sql(self) -> str:
        """SQL for inserting a new entry; ``ReplacingMergeTree`` deduplicates on read."""
        return f"""
            INSERT INTO {self._table_name}
                (collection, key, value, created_at, expires_at, version)
            VALUES
                ({{collection:String}}, {{key:String}}, {{value:String}},
                 {{created_at:String}}, {{expires_at:String}}, {{version:Int32}})
        """  # noqa: S608

    def _get_delete_sql(self) -> str:
        """SQL for deleting an entry by ``(collection, key)``."""
        return f"""
            DELETE FROM {self._table_name}
            WHERE collection = {{collection:String}} AND key = {{key:String}}
        """  # noqa: S608

    def _get_exists_sql(self) -> str:
        """SQL for checking whether an entry exists by ``(collection, key)``."""
        return f"""
            SELECT 1
            FROM {self._table_name} FINAL
            WHERE collection = {{collection:String}} AND key = {{key:String}}
            LIMIT 1
        """  # noqa: S608

    @staticmethod
    def _parse_jsoneachrow(text: str) -> list[dict[str, Any]]:
        """Parse the JSONEachRow output into a list of dicts."""
        if not text:
            return []
        return [json.loads(line) for line in text.splitlines() if line]

    def _execute(self, sql: str, params: dict[str, Any] | None = None) -> object:
        """Run a query, applying chDB string-parameter escaping."""
        return self._session.query(sql, "JSONEachRow", params=self._escape_params(params))  # pyright: ignore[reportUnknownMemberType]

    def _query_jsoneachrow(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Run a query and parse JSONEachRow output."""
        return self._parse_jsoneachrow(str(self._execute(sql, params)))

    @staticmethod
    def _escape_params(params: dict[str, Any] | None) -> dict[str, Any]:
        """Apply chDB string-literal escaping to any string-typed parameter values."""
        if not params:
            return {}
        return {k: (_escape_string_param(v) if isinstance(v, str) else v) for k, v in params.items()}

    @override
    async def _setup(self) -> None:
        """Initialize the database schema for key-value storage."""
        if not self._client_provided_by_user:
            self._exit_stack.callback(self._session.close)

        table_exists_sql = """
            SELECT name FROM system.tables
            WHERE database = currentDatabase() AND name = {table_name:String}
        """
        rows = self._query_jsoneachrow(table_exists_sql, params={"table_name": self._table_name})
        table_exists = len(rows) > 0

        if not table_exists:
            if not self._auto_create:
                msg = f"Table '{self._table_name}' does not exist. Either create the table manually or set auto_create=True."
                raise ValueError(msg)
            self._execute(self._get_create_table_sql())

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Fetch and deserialize the latest entry for ``(collection, key)``."""
        rows = self._query_jsoneachrow(
            self._get_select_sql(),
            params={"collection": collection, "key": key},
        )
        if not rows:
            return None

        row = rows[0]

        document: dict[str, Any] = {
            "value": row.get("value"),
            "created_at": row.get("created_at"),
            "expires_at": row.get("expires_at"),
            "version": row.get("version"),
        }

        try:
            return self._serialization_adapter.load_dict(data=document)
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
        """Insert a new row for ``(collection, key)``; older rows are merged out by chDB."""
        # Ensure that the value is serializable to JSON
        _ = managed_entry.value_as_json

        document = self._serialization_adapter.dump_dict(entry=managed_entry, key=key, collection=collection)

        self._execute(
            self._get_insert_sql(),
            params={
                "collection": collection,
                "key": key,
                "value": document["value"],
                "created_at": document.get("created_at") or "",
                "expires_at": document.get("expires_at") or "",
                "version": document.get("version", 1),
            },
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete the entry for ``(collection, key)``, returning whether it existed."""
        # ClickHouse's lightweight DELETE does not report row counts, so check
        # existence first.
        exists_rows = self._query_jsoneachrow(
            self._get_exists_sql(),
            params={"collection": collection, "key": key},
        )
        if not exists_rows:
            return False

        self._execute(
            self._get_delete_sql(),
            params={"collection": collection, "key": key},
        )
        return True
