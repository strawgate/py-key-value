import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from rocksdict import Rdict
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.rocksdb import RocksDBStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestRocksDBStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> RocksDBStore:
        return RocksDBStore(path=per_test_temp_dir / "test_db")

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
        # Close the user-provided database before cleanup
        db.close()
        temp_dir.cleanup()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @pytest.fixture
    async def rocksdb_client(self, store: RocksDBStore) -> Rdict:
        return store._db  # pyright: ignore[reportPrivateUsage]

    async def test_value_stored(self, store: RocksDBStore, rocksdb_client: Rdict):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        value = rocksdb_client.get(key="test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        value = rocksdb_client.get(key="test::test_key")
        assert value is not None
        value_as_dict = json.loads(value.decode("utf-8"))
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "value": {"age": 30, "name": "Alice"},
                "key": "test_key",
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )
