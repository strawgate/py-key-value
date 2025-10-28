from collections.abc import AsyncGenerator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.duckdb import DuckDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


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


class TestDuckDBStoreTextMode(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test DuckDB store with TEXT column mode instead of JSON."""

    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[DuckDBStore, None]:
        """Test with in-memory DuckDB database using TEXT column."""
        duckdb_store = DuckDBStore(use_json_column=False)
        yield duckdb_store
        await duckdb_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...


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
        store = DuckDBStore(use_json_column=True)

        # Store some test data with known metadata
        await store.put(collection="products", key="item1", value={"name": "Widget", "price": 10.99}, ttl=3600)
        await store.put(collection="products", key="item2", value={"name": "Gadget", "price": 25.50}, ttl=7200)
        await store.put(collection="orders", key="order1", value={"total": 100.00, "items": 3})

        # Query directly via SQL to verify native storage
        # Check that value is stored as JSON (can extract fields)
        result = store._connection.execute("""
            SELECT key, value->'name' as name, value->'price' as price
            FROM kv_entries
            WHERE collection = 'products'
            ORDER BY key
        """).fetchall()

        assert len(result) == 2
        assert result[0][0] == "item1"
        assert result[0][1] == '"Widget"'  # JSON strings are quoted
        assert result[1][0] == "item2"

        # Query by expiration timestamp
        result = store._connection.execute("""
            SELECT COUNT(*)
            FROM kv_entries
            WHERE expires_at > now() OR expires_at IS NULL
        """).fetchone()

        assert result[0] == 3  # All 3 entries should not be expired

        # Query metadata columns directly
        result = store._connection.execute("""
            SELECT key, ttl, created_at IS NOT NULL as has_created
            FROM kv_entries
            WHERE collection = 'products' AND ttl > 3600
        """).fetchall()

        assert len(result) == 1  # Only item2 has ttl > 3600
        assert result[0][0] == "item2"
        assert result[0][1] == 7200
        assert result[0][2] is True  # has_created

        await store.close()

    async def test_text_mode_storage(self):
        """Test that TEXT mode stores value as string instead of native JSON."""
        store = DuckDBStore(use_json_column=False)

        await store.put(collection="test", key="key1", value={"data": "value"})

        # Query to check column type - in TEXT mode, value should be a string
        result = store._connection.execute("""
            SELECT value, typeof(value) as value_type
            FROM kv_entries
            WHERE collection = 'test' AND key = 'key1'
        """).fetchone()

        assert result is not None
        value_str, value_type = result

        # In TEXT mode, value should be stored as VARCHAR/TEXT
        assert value_type in ("VARCHAR", "TEXT")
        # Value should be a JSON string
        assert isinstance(value_str, str)
        assert "data" in value_str

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

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...
