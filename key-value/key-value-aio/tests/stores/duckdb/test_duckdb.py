from collections.abc import AsyncGenerator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from _duckdb import DuckDBPyConnection
from duckdb import CatalogException
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.duckdb import DuckDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


def get_client_from_store(store: DuckDBStore) -> DuckDBPyConnection:
    return store._connection  # pyright: ignore[reportPrivateUsage]


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestDuckDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DuckDBStore, None]:
        """Test with in-memory DuckDB database."""
        duckdb_store = DuckDBStore()
        yield duckdb_store
        await duckdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestDuckDBStorePersistent(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DuckDBStore, None]:
        """Test with persistent DuckDB database file."""
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            duckdb_store = DuckDBStore(database_path=db_path)
            yield duckdb_store
            await duckdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestDuckDBStoreTextMode(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test DuckDB store with TEXT column mode (stringified JSON) instead of native JSON."""

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DuckDBStore, None]:
        """Test with in-memory DuckDB database using TEXT column for stringified JSON."""
        duckdb_store = DuckDBStore(native_storage=False)
        yield duckdb_store
        await duckdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestDuckDBStoreSpecific:
    """Test DuckDB-specific functionality."""

    @pytest.fixture
    async def store(self) -> AsyncGenerator[DuckDBStore, None]:
        """Provide DuckDB store instance."""
        duckdb_store = DuckDBStore()
        yield duckdb_store
        await duckdb_store.close()

    async def test_native_sql_queryability(self):
        """Test that users can query the database directly with SQL."""
        store = DuckDBStore(native_storage=True)

        # Store some test data with known metadata
        await store.put(collection="products", key="item1", value={"name": "Widget", "price": 10.99}, ttl=3600)
        await store.put(collection="products", key="item2", value={"name": "Gadget", "price": 25.50}, ttl=7200)
        await store.put(collection="orders", key="order1", value={"total": 100.00, "items": 3})

        # Query directly via SQL to verify native storage
        # Check that value_dict is stored as JSON (can extract fields)
        result = (
            get_client_from_store(store)
            .execute("""
            SELECT key, value_dict->'name' as name, value_dict->'price' as price
            FROM kv_entries
            WHERE collection = 'products'
            ORDER BY key
        """)
            .fetchall()
        )  # pyright: ignore[reportPrivateUsage]

        assert len(result) == 2
        assert result[0][0] == "item1"
        assert result[0][1] == '"Widget"'  # JSON strings are quoted
        assert result[1][0] == "item2"

        # Query by expiration timestamp
        count_result = (
            get_client_from_store(store)
            .execute("""
            SELECT COUNT(*)
            FROM kv_entries
            WHERE expires_at > now() OR expires_at IS NULL
        """)
            .fetchone()
        )  # pyright: ignore[reportPrivateUsage]

        assert count_result is not None
        assert count_result[0] == 3  # All 3 entries should not be expired

        await store.close()

    async def test_text_mode_storage(self):
        """Test that TEXT mode stores value as stringified JSON instead of native JSON."""
        store = DuckDBStore(native_storage=False)

        await store.put(collection="test", key="key1", value={"data": "value"})

        # Query to check column type - in TEXT mode, value_json should be populated
        result = (
            get_client_from_store(store)
            .execute("""
            SELECT value_json, value_dict, typeof(value_json) as json_type, typeof(value_dict) as dict_type
            FROM kv_entries
            WHERE collection = 'test' AND key = 'key1'
        """)
            .fetchone()
        )  # pyright: ignore[reportPrivateUsage]

        assert result is not None
        value_json, value_dict, json_type, _dict_type = result

        # In TEXT mode (native_storage=False), value_json should be populated, value_dict should be NULL
        assert value_json is not None
        assert value_dict is None
        assert json_type in ("VARCHAR", "TEXT")
        # Value should be a JSON string
        assert isinstance(value_json, str)
        assert "data" in value_json

        await store.close()

    async def test_database_path_initialization(self):
        """Test that store can be initialized with different database path options."""
        # In-memory (default)
        store1 = DuckDBStore()
        await store1.put(collection="test", key="key1", value={"test": "value1"})
        result1 = await store1.get(collection="test", key="key1")
        assert result1 == {"test": "value1"}
        await store1.close()

        # Explicit in-memory
        store2 = DuckDBStore(database_path=":memory:")
        await store2.put(collection="test", key="key2", value={"test": "value2"})
        result2 = await store2.get(collection="test", key="key2")
        assert result2 == {"test": "value2"}
        await store2.close()

    async def test_persistent_database(self):
        """Test that data persists across store instances when using file database."""
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "persist_test.db"

            # Store data in first instance
            store1 = DuckDBStore(database_path=db_path)
            await store1.put(collection="test", key="persist_key", value={"data": "persistent"})
            await store1.close()

            # Create second instance with same database file
            store2 = DuckDBStore(database_path=db_path)
            result = await store2.get(collection="test", key="persist_key")
            await store2.close()

            assert result == {"data": "persistent"}

    async def test_sql_injection_protection(self, store: DuckDBStore):
        """Test that the store is protected against SQL injection attacks."""
        malicious_collection = "test'; DROP TABLE kv_entries; --"
        malicious_key = "key'; DELETE FROM kv_entries; --"

        # These operations should not cause SQL injection
        await store.put(collection=malicious_collection, key=malicious_key, value={"safe": "data"})
        result = await store.get(collection=malicious_collection, key=malicious_key)
        assert result == {"safe": "data"}

        # Verify the table still exists and other data is safe
        await store.put(collection="normal", key="normal_key", value={"normal": "data"})
        normal_result = await store.get(collection="normal", key="normal_key")
        assert normal_result == {"normal": "data"}

    async def test_large_data_storage(self, store: DuckDBStore):
        """Test storing and retrieving large data values."""
        # Create a large value (1MB of data)
        large_value = {"large_data": "x" * (1024 * 1024)}

        await store.put(collection="test", key="large_key", value=large_value)
        result = await store.get(collection="test", key="large_key")

        assert result == large_value

    async def test_unicode_support(self, store: DuckDBStore):
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
        result = await store.get(collection="unicode_test", key="unicode_key")

        assert result == unicode_data

    async def test_connection_initialization(self):
        """Test that store can be initialized with existing DuckDB connection."""
        import duckdb

        conn = duckdb.connect(":memory:")
        store = DuckDBStore(connection=conn)

        await store.put(collection="test", key="conn_test", value={"test": "value"})
        result = await store.get(collection="test", key="conn_test")
        assert result == {"test": "value"}

        await store.close()

    async def test_custom_table_name(self):
        """Test that store can use custom table name."""
        custom_table = "my_custom_kv_table"
        store = DuckDBStore(table_name=custom_table)

        # Store some data
        await store.put(collection="test", key="key1", value={"data": "value"})

        # Verify the custom table exists and contains the data
        tables = (
            get_client_from_store(store)
            .table(custom_table)
            .filter(filter_expr="key = 'key1'")
            .select("key", "collection")
            .execute()
            .fetchone()
        )

        assert tables == snapshot(("key1", "test"))

        # Verify default table doesn't exist
        with pytest.raises(CatalogException):
            get_client_from_store(store).table("kv_entries")

        await store.close()

    async def test_native_vs_stringified_storage(self):
        """Test that native and stringified storage modes work correctly."""
        # Native storage (default)
        store_native = DuckDBStore(native_storage=True)
        await store_native.put(collection="test", key="key1", value={"name": "native"})

        result_native = (
            get_client_from_store(store_native)
            .execute("""
            SELECT value_dict, value_json
            FROM kv_entries
            WHERE key = 'key1'
        """)
            .fetchone()
        )  # pyright: ignore[reportPrivateUsage]

        assert result_native is not None
        assert result_native[0] is not None  # value_dict should be populated
        assert result_native[1] is None  # value_json should be NULL

        await store_native.close()

        # Stringified storage
        store_string = DuckDBStore(native_storage=False)
        await store_string.put(collection="test", key="key2", value={"name": "stringified"})

        result_string = (
            get_client_from_store(store_string)
            .execute("""
            SELECT value_dict, value_json
            FROM kv_entries
            WHERE key = 'key2'
        """)
            .fetchone()
        )  # pyright: ignore[reportPrivateUsage]

        assert result_string is not None
        assert result_string[0] is None  # value_dict should be NULL
        assert result_string[1] is not None  # value_json should be populated

        await store_string.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...
