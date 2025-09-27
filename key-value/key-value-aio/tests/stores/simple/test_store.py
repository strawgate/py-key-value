import pytest
from typing_extensions import override

from key_value.aio.stores.simple.store import SimpleStore
from tests.stores.conftest import BaseStoreTests


class TestSimpleStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SimpleStore:
        return SimpleStore(max_entries=500)
