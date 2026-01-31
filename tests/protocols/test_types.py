from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.memory import MemoryStore


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
