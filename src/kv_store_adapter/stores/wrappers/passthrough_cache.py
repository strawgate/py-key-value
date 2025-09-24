from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo


class PassthroughCacheWrapper(BaseKVStore):
    """Wrapper that users two stores, ideal for combining a local and distributed store."""

    def __init__(self, primary_store: BaseKVStore, cache_store: BaseKVStore) -> None:
        """Initialize the passthrough cache wrapper. Items are first checked in the primary store and if not found, are
        checked in the secondary store. Operations are performed on both stores but are not atomic.

        Operations like expiry culling against the primary store will not be reflected in the cache store. This may
        lead to stale data in the cache store. One way to combat this is to use a TTLClampWrapper on the cache store to
        enforce a lower TTL on the cache store than the primary store.

        Args:
            primary_store: The primary store the data will live in.
            cache_store: The write-through (likely ephemeral) cache to use.
        """
        self.cache_store: BaseKVStore = cache_store
        self.primary_store: BaseKVStore = primary_store

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        if cache_store_value := await self.cache_store.get(collection=collection, key=key):
            return cache_store_value

        if primary_store_value := await self.primary_store.get(collection=collection, key=key):
            ttl_info: TTLInfo | None = await self.primary_store.ttl(collection=collection, key=key)

            await self.cache_store.put(collection=collection, key=key, value=primary_store_value, ttl=ttl_info.ttl if ttl_info else None)

            return primary_store_value
        return None

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        _ = await self.cache_store.delete(collection=collection, key=key)
        await self.primary_store.put(collection=collection, key=key, value=value, ttl=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        deleted = await self.primary_store.delete(collection=collection, key=key)
        _ = await self.cache_store.delete(collection=collection, key=key)
        return deleted

    @override
    async def exists(self, collection: str, key: str) -> bool:
        return await self.get(collection=collection, key=key) is not None

    @override
    async def keys(self, collection: str) -> list[str]:
        return await self.primary_store.keys(collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        removed: int = await self.primary_store.clear_collection(collection=collection)
        _ = await self.cache_store.clear_collection(collection=collection)
        return removed

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        if ttl_info := await self.cache_store.ttl(collection=collection, key=key):
            return ttl_info

        return await self.primary_store.ttl(collection=collection, key=key)

    @override
    async def list_collections(self) -> list[str]:
        collections: list[str] = await self.primary_store.list_collections()

        return collections

    @override
    async def cull(self) -> None:
        await self.primary_store.cull()
        await self.cache_store.cull()
