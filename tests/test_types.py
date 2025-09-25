from datetime import datetime, timedelta, timezone

from kv_store_adapter.stores.memory import MemoryStore
from kv_store_adapter.types import KVStore, TTLInfo


def test_ttl_info():
    created_at = datetime.now(tz=timezone.utc)
    expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=100)
    ttl_info = TTLInfo(collection="test", key="test", created_at=created_at, ttl=100, expires_at=expires_at)

    assert ttl_info.expires_at is not None
    assert ttl_info.expires_at > datetime.now(tz=timezone.utc)
    assert ttl_info.expires_at < datetime.now(tz=timezone.utc) + timedelta(seconds=100)

    assert ttl_info.created_at is not None
    assert ttl_info.created_at < datetime.now(tz=timezone.utc)
    assert ttl_info.created_at > datetime.now(tz=timezone.utc) - timedelta(seconds=5)

    assert ttl_info.collection == "test"
    assert ttl_info.key == "test"

    assert ttl_info.is_expired is False


async def test_kv_store_protocol():
    async def test_kv_store_protocol(kv_store: KVStore):
        assert await kv_store.get(collection="test", key="test") is None
        await kv_store.put(collection="test", key="test", value={"test": "test"})
        assert await kv_store.delete(collection="test", key="test")
        await kv_store.put(collection="test", key="test_2", value={"test": "test"})

    memory_store = MemoryStore()

    await test_kv_store_protocol(kv_store=memory_store)

    assert await memory_store.get(collection="test", key="test") is None
    assert await memory_store.get(collection="test", key="test_2") == {"test": "test"}
