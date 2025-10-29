from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from inline_snapshot import snapshot
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.rocksdb import RocksDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


def test_managed_entry_serialization():
    """Test ManagedEntry serialization to JSON for RocksDB storage."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    json_str = managed_entry.to_json()

    assert json_str == snapshot('{"value": {"test": "test"}}')

    round_trip_managed_entry = ManagedEntry.from_json(json_str=json_str)

    assert round_trip_managed_entry.value == managed_entry.value


class TestRocksDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[RocksDBStore, None]:
        """Create a RocksDB store for testing."""
        # Create a temporary directory for the RocksDB database
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_db"
            rocksdb_store = RocksDBStore(path=db_path)
            yield rocksdb_store

    async def test_rocksdb_path_connection(self):
        """Test RocksDB store creation with path."""
        temp_dir = TemporaryDirectory()
        db_path = Path(temp_dir.name) / "path_test_db"

        store = RocksDBStore(path=db_path)

        await store.put(collection="test", key="path_test", value={"test": "value"})
        result = await store.get(collection="test", key="path_test")
        assert result == {"test": "value"}

        await store.close()
        temp_dir.cleanup()

    async def test_rocksdb_db_connection(self):
        """Test RocksDB store creation with existing DB instance."""
        from rocksdict import Options, Rdict

        temp_dir = TemporaryDirectory()
        db_path = Path(temp_dir.name) / "db_test_db"
        db_path.mkdir(parents=True, exist_ok=True)

        opts = Options()
        opts.create_if_missing(True)
        db = Rdict(str(db_path), options=opts)

        store = RocksDBStore(db=db)

        await store.put(collection="test", key="db_test", value={"test": "value"})
        result = await store.get(collection="test", key="db_test")
        assert result == {"test": "value"}

        await store.close()
        temp_dir.cleanup()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored_as_bytes(self, store: RocksDBStore):
        """Verify values are stored as JSON bytes in RocksDB."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get raw RocksDB value using the compound key format
        combo_key = compound_key(collection="test", key="test_key")
        raw_value = store._db.get(combo_key)  # pyright: ignore[reportPrivateUsage]

        # Decode bytes to string
        assert isinstance(raw_value, bytes)
        decoded_value = raw_value.decode("utf-8")

        assert decoded_value == snapshot('{"value": {"name": "Alice", "age": 30}}')
