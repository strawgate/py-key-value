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

    @pytest.mark.parametrize("updated_key", ["first", "second"])
    async def test_update_at_capacity_preserves_other_entries(self, updated_key: str):
        store = SimpleStore(max_entries=2)
        await store.put("first", {"value": 1})
        await store.put("second", {"value": 2})
        await store.put(updated_key, {"value": 3})

        expected = [{"value": 3 if updated_key == "first" else 1}, {"value": 3 if updated_key == "second" else 2}]
        assert await store.get_many(["first", "second"]) == expected

        await store.put("third", {"value": 4})
        assert await store.get("first") is None
        assert await store.get("second") == expected[1]
        assert await store.get("third") == {"value": 4}

    async def test_update_at_capacity_preserves_same_key_in_another_collection(self):
        store = SimpleStore(max_entries=2)
        await store.put("key", {"value": 1}, collection="first")
        await store.put("key", {"value": 2}, collection="second")
        await store.put("key", {"value": 3}, collection="second")

        assert await store.get("key", collection="first") == {"value": 1}
        assert await store.get("key", collection="second") == {"value": 3}
