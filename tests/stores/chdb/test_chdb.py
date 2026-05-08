from collections.abc import AsyncGenerator

import pytest

# Skip the whole module if chdb is unavailable (e.g., Windows has no wheels).
pytest.importorskip("chdb")

from chdb.session import Session
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.chdb import ChDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


def get_session_from_store(store: ChDBStore) -> Session:
    """Return the underlying chDB session for direct SQL access in tests."""
    return store._session


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test ChDBStore with the default in-memory database."""

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ChDBStore, None]:
        """Test with in-memory chDB database."""
        chdb_store = ChDBStore()
        yield chdb_store
        await chdb_store.close()

    @pytest.mark.skip(reason="In-memory chDB store is unbounded")
    async def test_not_unbounded(self, store: BaseStore):
        """chDB does not enforce a per-store storage bound."""


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStoreCustomTable(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test ChDBStore with a non-default table name to exercise table-name isolation.

    chDB uses a process-global embedded server, so we cannot use a different
    ``database_path`` while other in-memory sessions are active in the same
    process. Instead, we use a unique table name to exercise the same code paths
    while remaining compatible with single-worker test execution.
    """

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ChDBStore, None]:
        """Test with a dedicated custom-named table."""
        chdb_store = ChDBStore(table_name="kv_custom_table_tests")
        yield chdb_store
        await chdb_store.close()

    @pytest.mark.skip(reason="In-memory chDB store is unbounded")
    async def test_not_unbounded(self, store: BaseStore):
        """chDB does not enforce a per-store storage bound."""


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStoreSpecific:
    """Test chDB-specific functionality."""

    @pytest.fixture
    async def store(self) -> AsyncGenerator[ChDBStore, None]:
        """Provide an in-memory ChDBStore for chDB-specific tests."""
        chdb_store = ChDBStore()
        yield chdb_store
        await chdb_store.close()

    async def test_database_path_initialization(self):
        """Test that different table names provide isolation within the same session."""
        store1 = ChDBStore(table_name="store_one")
        store2 = ChDBStore(table_name="store_two")

        # Write to store1 only
        await store1.put(collection="test", key="key1", value={"from": "store1"})

        # store2 should NOT see store1's data (different tables)
        assert await store2.get(collection="test", key="key1") is None

        # Each store works independently
        await store2.put(collection="test", key="key2", value={"from": "store2"})
        assert await store2.get(collection="test", key="key2") == {"from": "store2"}
        assert await store1.get(collection="test", key="key2") is None

        await store1.close()
        await store2.close()

    async def test_persistent_database(self):
        """Test that data persists within the same chDB session across store instances.

        Note: chDB uses a process-global embedded server. We use a shared session
        to show that data written by one store instance is visible to another.
        """
        from chdb.session import Session

        session = Session(":memory:")
        table = "persist_test_table"

        store1 = ChDBStore(session=session, table_name=table)
        await store1.put(collection="test", key="persist_key", value={"data": "persistent"})
        await store1.close()

        # Second store instance with the same session can read the data
        store2 = ChDBStore(session=session, table_name=table)
        result = await store2.get(collection="test", key="persist_key")
        await store2.close()

        assert result == {"data": "persistent"}
        session.close()

    async def test_auto_create_false_raises_when_table_missing(self):
        """Test that auto_create=False raises StoreSetupError when table doesn't exist."""
        from key_value.aio.errors import StoreSetupError

        store = ChDBStore(table_name="nonexistent_table_xyz", auto_create=False)
        with pytest.raises(StoreSetupError, match="does not exist"):
            await store.put(collection="test", key="k", value={"v": 1})

    async def test_context_manager_usage(self):
        """Test that the store works correctly as an async context manager."""
        async with ChDBStore() as store:
            await store.put(collection="test", key="ctx_key", value={"ctx": "value"})
            result = await store.get(collection="test", key="ctx_key")
            assert result == {"ctx": "value"}

    async def test_native_sql_queryability(self):
        """Test that users can query the database directly with SQL."""
        async with ChDBStore() as store:
            await store.put(collection="products", key="item1", value={"name": "Widget", "price": 10.99}, ttl=3600)
            await store.put(collection="products", key="item2", value={"name": "Gadget", "price": 25.50}, ttl=7200)
            await store.put(collection="orders", key="order1", value={"total": 100.00, "items": 3})

            # Query directly via SQL to verify native storage and access
            rows = store._query_jsoneachrow(
                f"SELECT key, value FROM {store._table_name} FINAL WHERE collection = {{collection:String}} ORDER BY key",  # noqa: S608
                params={"collection": "products"},
            )

            assert len(rows) == 2
            assert rows[0]["key"] == "item1"
            assert rows[1]["key"] == "item2"

            # Verify we can count entries per collection
            count_rows = store._query_jsoneachrow(
                f"SELECT count() as cnt FROM {store._table_name} FINAL WHERE collection = {{collection:String}}",  # noqa: S608
                params={"collection": "products"},
            )
            assert int(count_rows[0]["cnt"]) == 2

    async def test_sql_injection_protection(self, store: ChDBStore):
        """Test that the store is protected against SQL injection attacks."""
        malicious_collection = "test'; DROP TABLE kv_entries; --"
        malicious_key = "key'; DELETE FROM kv_entries; --"

        await store.put(collection=malicious_collection, key=malicious_key, value={"safe": "data"})
        assert await store.get(collection=malicious_collection, key=malicious_key) == {"safe": "data"}

        await store.put(collection="normal", key="normal_key", value={"normal": "data"})
        assert await store.get(collection="normal", key="normal_key") == {"normal": "data"}

    async def test_unicode_support(self, store: ChDBStore):
        """Test that the store properly handles Unicode characters."""
        unicode_data = {
            "english": "Hello World",
            "chinese": "你好世界",
            "japanese": "こんにちは世界",
            "arabic": "مرحبا بالعالم",
            "emoji": "🌍🚀💻",
            "special": "Special chars: !@#$%^&*()_+-={}[]|\\:;\"'<>?,./",
        }

        await store.put(collection="unicode_test", key="unicode_key", value=unicode_data)
        assert await store.get(collection="unicode_test", key="unicode_key") == unicode_data

    async def test_session_initialization(self):
        """Test that store can be initialized with an existing chDB session."""
        from chdb.session import Session

        session = Session(":memory:")
        store = ChDBStore(session=session)

        await store.put(collection="test", key="conn_test", value={"test": "value"})
        assert await store.get(collection="test", key="conn_test") == {"test": "value"}

        await store.close()
        # The user-provided session should still be usable.
        result = session.query("SELECT 1", "JSONEachRow")  # pyright: ignore[reportUnknownMemberType]
        assert "1" in str(result)
        session.close()

    async def test_custom_table_name(self):
        """Test that store can use custom table name."""
        custom_table = "my_custom_kv_table"
        store = ChDBStore(table_name=custom_table)

        await store.put(collection="test", key="key1", value={"data": "value"})

        rows = get_session_from_store(store).query(  # pyright: ignore[reportUnknownMemberType]
            f"SELECT key, collection FROM {custom_table} FINAL WHERE key = 'key1'",  # noqa: S608
            "JSONEachRow",
        )
        assert "key1" in str(rows)
        assert "test" in str(rows)

        await store.close()

    async def test_invalid_table_name_rejected(self):
        """Test that table names containing unsafe characters are rejected at init."""
        with pytest.raises(ValueError, match="Table name"):
            ChDBStore(table_name="bad name; DROP TABLE")

    async def test_session_and_path_mutually_exclusive(self):
        """Test that providing both ``session`` and ``database_path`` is an error."""
        from chdb.session import Session

        session = Session(":memory:")
        try:
            with pytest.raises(ValueError, match="Provide only one"):
                ChDBStore(session=session, database_path=":memory:")  # pyright: ignore[reportCallIssue]
        finally:
            session.close()

    async def test_replace_returns_latest_value(self, store: ChDBStore):
        """ReplacingMergeTree+FINAL should return the most recent write."""
        await store.put(collection="c", key="k", value={"v": 1})
        await store.put(collection="c", key="k", value={"v": 2})
        await store.put(collection="c", key="k", value={"v": 3})
        assert await store.get(collection="c", key="k") == {"v": 3}

    async def test_large_data_storage(self, store: ChDBStore):
        """Test storing and retrieving large data values."""
        large_value = {"large_data": "x" * (1024 * 1024)}

        await store.put(collection="test", key="large_key", value=large_value)
        result = await store.get(collection="test", key="large_key")

        assert result == large_value
