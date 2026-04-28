from collections.abc import AsyncGenerator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from chdb.session import Session
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.chdb import ChDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


def get_session_from_store(store: ChDBStore) -> Session:
    return store._session


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[ChDBStore, None]:
        """Test with in-memory chDB database."""
        chdb_store = ChDBStore()
        yield chdb_store
        await chdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStorePersistent(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> AsyncGenerator[ChDBStore, None]:
        """Test with persistent chDB database directory."""
        chdb_store = ChDBStore(database_path=per_test_temp_dir / "chdb_data")
        yield chdb_store
        await chdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestChDBStoreSpecific:
    """Test chDB-specific functionality."""

    @pytest.fixture
    async def store(self) -> AsyncGenerator[ChDBStore, None]:
        chdb_store = ChDBStore()
        yield chdb_store
        await chdb_store.close()

    async def test_database_path_initialization(self):
        """Test that store can be initialized with different database path options."""
        store1 = ChDBStore()
        await store1.put(collection="test", key="key1", value={"test": "value1"})
        assert await store1.get(collection="test", key="key1") == {"test": "value1"}
        await store1.close()

        store2 = ChDBStore(database_path=":memory:")
        await store2.put(collection="test", key="key2", value={"test": "value2"})
        assert await store2.get(collection="test", key="key2") == {"test": "value2"}
        await store2.close()

    async def test_persistent_database(self):
        """Test that data persists across store instances when using a directory database."""
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "persist_data"

            store1 = ChDBStore(database_path=db_path)
            await store1.put(collection="test", key="persist_key", value={"data": "persistent"})
            await store1.close()

            store2 = ChDBStore(database_path=db_path)
            result = await store2.get(collection="test", key="persist_key")
            await store2.close()

            assert result == {"data": "persistent"}

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
        with pytest.raises(ValueError, match="Table name"):
            ChDBStore(table_name="bad name; DROP TABLE")

    async def test_session_and_path_mutually_exclusive(self):
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

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...
