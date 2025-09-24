from typing import Any

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo


class SingleCollectionAdapter:
    """Adapter around a KV Store that only allows one collection."""

    def __init__(self, store: BaseKVStore, collection: str) -> None:
        self.store: BaseKVStore = store
        self.collection: str = collection

    async def get(self, key: str) -> dict[str, Any] | None:
        return await self.store.get(collection=self.collection, key=key)

    async def put(self, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        await self.store.put(collection=self.collection, key=key, value=value, ttl=ttl)

    async def delete(self, key: str) -> bool:
        return await self.store.delete(collection=self.collection, key=key)

    async def exists(self, key: str) -> bool:
        return await self.store.exists(collection=self.collection, key=key)

    async def keys(self) -> list[str]:
        return await self.store.keys(collection=self.collection)

    async def ttl(self, key: str) -> TTLInfo | None:
        return await self.store.ttl(collection=self.collection, key=key)
