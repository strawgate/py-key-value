import pytest
from dirty_equals import IsFloat
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.default_value import DefaultValueWrapper
from tests.stores.base import BaseStoreTests

TEST_KEY_1 = "test_key_1"
TEST_KEY_2 = "test_key_2"
TEST_COLLECTION = "test_collection"
DEFAULT_VALUE = {"obj_key": "obj_value"}
DEFAULT_TTL = 100


class TestDefaultValueWrapper(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, memory_store: MemoryStore) -> DefaultValueWrapper:
        return DefaultValueWrapper(key_value=memory_store, default_value=DEFAULT_VALUE, default_ttl=DEFAULT_TTL)

    async def test_default_value(self, store: BaseStore):
        assert await store.get(collection=TEST_COLLECTION, key=TEST_KEY_1) == DEFAULT_VALUE
        assert await store.ttl(collection=TEST_COLLECTION, key=TEST_KEY_1) == (DEFAULT_VALUE, IsFloat(approx=DEFAULT_TTL))
        assert await store.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY_1, TEST_KEY_2]) == [DEFAULT_VALUE, DEFAULT_VALUE]
        assert await store.ttl_many(collection=TEST_COLLECTION, keys=[TEST_KEY_1, TEST_KEY_2]) == [
            (DEFAULT_VALUE, IsFloat(approx=DEFAULT_TTL)),
            (DEFAULT_VALUE, IsFloat(approx=DEFAULT_TTL)),
        ]

        await store.put(collection=TEST_COLLECTION, key=TEST_KEY_2, value={"key_2": "value_2"}, ttl=200)
        assert await store.get(collection=TEST_COLLECTION, key=TEST_KEY_2) == {"key_2": "value_2"}
        assert await store.ttl(collection=TEST_COLLECTION, key=TEST_KEY_2) == ({"key_2": "value_2"}, IsFloat(approx=200))
        assert await store.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY_1, TEST_KEY_2]) == [DEFAULT_VALUE, {"key_2": "value_2"}]
        assert await store.ttl_many(collection=TEST_COLLECTION, keys=[TEST_KEY_1, TEST_KEY_2]) == [
            (DEFAULT_VALUE, IsFloat(approx=DEFAULT_TTL)),
            ({"key_2": "value_2"}, IsFloat(approx=200)),
        ]

    async def test_default_value_returns_fresh_dicts(self, store: BaseStore):
        first_result = await store.get(collection=TEST_COLLECTION, key=TEST_KEY_1)
        assert first_result == DEFAULT_VALUE

        assert first_result is not None
        first_result["obj_key"] = "mutated"

        second_result = await store.get(collection=TEST_COLLECTION, key=TEST_KEY_1)
        assert second_result == DEFAULT_VALUE
        assert second_result is not first_result

    async def test_default_value_get_many_returns_independent_dicts(self, store: BaseStore):
        results = await store.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY_1, TEST_KEY_2])
        assert results == [DEFAULT_VALUE, DEFAULT_VALUE]
        assert results[0] is not None
        assert results[1] is not None

        results[0]["obj_key"] = "mutated"
        assert results[1] == DEFAULT_VALUE

    @override
    @pytest.mark.skip
    async def test_empty_get(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_put_get_many_missing_one(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_empty_ttl(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_get_put_get(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_get_put_get_delete_get(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_get_delete_get(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_many_get_get_delete_many_get_many(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_many_get_many_delete_many_get_many(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_get_put_get_put_delete_get(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_many_delete_delete_get_many(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_put_expired_get_none(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    @pytest.mark.skip
    async def test_concurrent_operations(self, store: BaseStore): ...
