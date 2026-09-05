import pytest

from key_value.aio.protocols.key_value import (
    AsyncKeyValue,
    AsyncPutIfAbsentProtocol,
)
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.stores.null import NullStore


async def test_key_value_protocol():
    async def test_protocol(key_value: AsyncKeyValue):
        assert await key_value.get(collection="test", key="test") is None
        await key_value.put(collection="test", key="test", value={"test": "test"})
        assert await key_value.delete(collection="test", key="test")
        await key_value.put(collection="test", key="test_2", value={"test": "test"})

    memory_store = MemoryStore()

    await test_protocol(key_value=memory_store)

    assert await memory_store.get(collection="test", key="test") is None
    assert await memory_store.get(collection="test", key="test_2") == {"test": "test"}


def test_put_if_absent_is_an_optional_protocol():
    assert isinstance(MemoryStore(), AsyncPutIfAbsentProtocol)
    with pytest.warns(UserWarning, match="configured store is unstable"):
        null_store = NullStore()
    assert not isinstance(null_store, AsyncPutIfAbsentProtocol)
