from key_value.aio.stores.memory import MemoryStore
from key_value.aio.types import AsyncKeyValue


async def test_kv_store_protocol():
    async def test_protocol(kv_store: AsyncKeyValue):
        assert await kv_store.get(collection="test", key="test") is None
        await kv_store.put(collection="test", key="test", value={"test": "test"})
        assert await kv_store.delete(collection="test", key="test")
        await kv_store.put(collection="test", key="test_2", value={"test": "test"})

    memory_store = MemoryStore()

    await test_protocol(kv_store=memory_store)

    assert await memory_store.get(collection="test", key="test") is None
    assert await memory_store.get(collection="test", key="test_2") == {"test": "test"}
