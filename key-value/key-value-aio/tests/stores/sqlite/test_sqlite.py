from collections.abc import AsyncGenerator
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.sqlite import SQLiteStore
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin


class TestSQLiteStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> AsyncGenerator[SQLiteStore, None]:
        """Create a SQLite store for testing."""
        # Create a temporary directory for the SQLite database
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            sqlite_store = SQLiteStore(path=db_path)
            yield sqlite_store
            await sqlite_store.close()

    @pytest.mark.skip(reason="Local disk stores are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
