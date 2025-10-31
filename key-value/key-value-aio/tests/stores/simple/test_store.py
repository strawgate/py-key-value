import pytest
from typing_extensions import override

from key_value.aio.stores.simple.store import SimpleStore
from tests.stores.base import BaseStoreTests


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestSimpleStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> SimpleStore:
        return SimpleStore(max_entries=500)
