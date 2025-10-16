from collections.abc import AsyncGenerator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.rocksdb import RocksDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


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
