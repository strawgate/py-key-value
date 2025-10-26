import pytest
from dirty_equals import IsFloat
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.routing import CollectionRoutingWrapper, RoutingWrapper
from tests.stores.base import BaseStoreTests

KEY_ONE = "key1"
VALUE_ONE = {"this_key_1": "this_value_1"}
COLLECTION_ONE = "first"

KEY_TWO = "key2"
VALUE_TWO = {"this_key_2": "this_value_2"}
COLLECTION_TWO = "second"

KEY_UNMAPPED = "key3"
VALUE_UNMAPPED = {"this_key_3": "this_value_3"}
COLLECTION_UNMAPPED = "unmapped"

ALL_KEYS = [KEY_ONE, KEY_TWO, KEY_UNMAPPED]


class TestRoutingWrapper(BaseStoreTests):
    @pytest.fixture
    def second_store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    def default_store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    def store(self, memory_store: MemoryStore, second_store: MemoryStore, default_store: MemoryStore) -> RoutingWrapper:
        first_store = memory_store

        def route(collection: str | None) -> AsyncKeyValue | None:
            if collection == COLLECTION_ONE:
                return first_store
            if collection == COLLECTION_TWO:
                return second_store
            return None

        return RoutingWrapper(routing_function=route, default_store=default_store)

    @pytest.fixture
    async def store_with_data(self, store: RoutingWrapper) -> RoutingWrapper:
        await store.put(key=KEY_ONE, value=VALUE_ONE, collection=COLLECTION_ONE)
        await store.put(key=KEY_TWO, value=VALUE_TWO, collection=COLLECTION_TWO)
        await store.put(key=KEY_UNMAPPED, value=VALUE_UNMAPPED, collection=COLLECTION_UNMAPPED)
        return store

    @override
    @pytest.mark.skip(reason="RoutingWrapper is unbounded")
    async def test_not_unbounded(self, store: RoutingWrapper): ...

    async def test_routing_get_and_get_many(
        self,
        store_with_data: RoutingWrapper,
        memory_store: MemoryStore,
        second_store: MemoryStore,
        default_store: MemoryStore,  # pyright: ignore[reportUnusedParameter]
    ):
        """Test basic routing sends gets"""
        assert await memory_store.get(key=KEY_ONE, collection=COLLECTION_ONE) == VALUE_ONE
        assert await memory_store.get(key=KEY_TWO, collection=COLLECTION_TWO) is None
        assert await memory_store.get(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) is None
        assert await memory_store.get_many(keys=ALL_KEYS, collection=COLLECTION_ONE) == [VALUE_ONE, None, None]

        assert await second_store.get(key=KEY_ONE, collection=COLLECTION_ONE) is None
        assert await second_store.get(key=KEY_TWO, collection=COLLECTION_TWO) == VALUE_TWO
        assert await second_store.get(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) is None
        assert await second_store.get_many(keys=ALL_KEYS, collection=COLLECTION_TWO) == [None, VALUE_TWO, None]

        assert await default_store.get(key=KEY_ONE, collection=COLLECTION_ONE) is None
        assert await default_store.get(key=KEY_TWO, collection=COLLECTION_TWO) is None
        assert await default_store.get(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) == VALUE_UNMAPPED
        assert await default_store.get_many(keys=ALL_KEYS, collection=COLLECTION_UNMAPPED) == [None, None, VALUE_UNMAPPED]

    async def test_routing_delete(
        self, store_with_data: RoutingWrapper, memory_store: MemoryStore, second_store: MemoryStore, default_store: MemoryStore
    ):
        """Test delete operations route correctly."""

        assert await store_with_data.get(key=KEY_ONE, collection=COLLECTION_ONE) == VALUE_ONE
        await store_with_data.delete(key=KEY_ONE, collection=COLLECTION_ONE)
        assert await memory_store.get(key=KEY_ONE, collection=COLLECTION_ONE) is None
        assert await memory_store.get_many(keys=ALL_KEYS, collection=COLLECTION_ONE) == [None, None, None]
        assert await second_store.get_many(keys=ALL_KEYS, collection=COLLECTION_ONE) == [None, None, None]
        assert await default_store.get_many(keys=ALL_KEYS, collection=COLLECTION_ONE) == [None, None, None]

        assert await store_with_data.get(key=KEY_TWO, collection=COLLECTION_TWO) == VALUE_TWO
        await store_with_data.delete(key=KEY_TWO, collection=COLLECTION_TWO)
        assert await memory_store.get(key=KEY_TWO, collection=COLLECTION_TWO) is None
        assert await memory_store.get_many(keys=ALL_KEYS, collection=COLLECTION_TWO) == [None, None, None]
        assert await second_store.get_many(keys=ALL_KEYS, collection=COLLECTION_TWO) == [None, None, None]
        assert await default_store.get_many(keys=ALL_KEYS, collection=COLLECTION_TWO) == [None, None, None]

        assert await store_with_data.get(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) == VALUE_UNMAPPED
        await store_with_data.delete(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED)
        assert await memory_store.get(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) is None
        assert await memory_store.get_many(keys=ALL_KEYS, collection=COLLECTION_UNMAPPED) == [None, None, None]
        assert await second_store.get_many(keys=ALL_KEYS, collection=COLLECTION_UNMAPPED) == [None, None, None]
        assert await default_store.get_many(keys=ALL_KEYS, collection=COLLECTION_UNMAPPED) == [None, None, None]

    async def test_routing_ttl(
        self, store: RoutingWrapper, memory_store: MemoryStore, second_store: MemoryStore, default_store: MemoryStore
    ):
        """Test TTL operations route correctly."""
        key_one_ttl = 1800
        key_two_ttl = 2700
        key_unmapped_ttl = 7200

        await store.put(key=KEY_ONE, value=VALUE_ONE, collection=COLLECTION_ONE, ttl=key_one_ttl)
        await store.put(key=KEY_TWO, value=VALUE_TWO, collection=COLLECTION_TWO, ttl=key_two_ttl)
        await store.put(key=KEY_UNMAPPED, value=VALUE_UNMAPPED, collection=COLLECTION_UNMAPPED, ttl=key_unmapped_ttl)

        assert await store.ttl(key=KEY_ONE, collection=COLLECTION_ONE) == (VALUE_ONE, IsFloat(approx=key_one_ttl))
        assert await store.ttl(key=KEY_TWO, collection=COLLECTION_TWO) == (VALUE_TWO, IsFloat(approx=key_two_ttl))
        assert await store.ttl(key=KEY_UNMAPPED, collection=COLLECTION_UNMAPPED) == (VALUE_UNMAPPED, IsFloat(approx=key_unmapped_ttl))


class TestCollectionRoutingWrapper(TestRoutingWrapper):
    @pytest.fixture
    def store(self, memory_store: MemoryStore, second_store: MemoryStore, default_store: MemoryStore) -> CollectionRoutingWrapper:
        return CollectionRoutingWrapper(
            collection_map={
                COLLECTION_ONE: memory_store,
                COLLECTION_TWO: second_store,
            },
            default_store=default_store,
        )
