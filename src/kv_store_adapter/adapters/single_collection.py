from typing import Any

from kv_store_adapter.types import KVStoreProtocol


class SingleCollectionAdapter:
    """Adapter around a KVStoreProtocol-compliant Store that only allows one collection."""

    def __init__(self, store: KVStoreProtocol, collection: str) -> None:
        self.store: KVStoreProtocol = store
        self.collection: str = collection

    async def get(self, key: str) -> dict[str, Any] | None:
        return await self.store.get(collection=self.collection, key=key)

    async def put(self, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        await self.store.put(collection=self.collection, key=key, value=value, ttl=ttl)

    async def delete(self, key: str) -> bool:
        return await self.store.delete(collection=self.collection, key=key)

    async def exists(self, key: str) -> bool:
        return await self.store.exists(collection=self.collection, key=key)
