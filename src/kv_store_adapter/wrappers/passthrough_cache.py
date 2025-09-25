from typing import Any

from kv_store_adapter.types import KVStoreProtocol


class PassthroughCacheWrapper:
    """Wrapper that uses two stores, ideal for combining a local and distributed store."""

    def __init__(self, primary_store: KVStoreProtocol, cache_store: KVStoreProtocol) -> None:
        """Initialize the passthrough cache wrapper. Items are first checked in the primary store and if not found, are
        checked in the secondary store. Operations are performed on both stores but are not atomic.

        Note: This wrapper only implements the core KVStoreProtocol operations. Operations like expiry culling 
        against the primary store will not be reflected in the cache store if the underlying stores support such operations.

        Args:
            primary_store: The primary store the data will live in.
            cache_store: The write-through (likely ephemeral) cache to use.
        """
        self.cache_store: KVStoreProtocol = cache_store
        self.primary_store: KVStoreProtocol = primary_store

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        if cache_store_value := await self.cache_store.get(collection=collection, key=key):
            return cache_store_value

        if primary_store_value := await self.primary_store.get(collection=collection, key=key):
            # Cache the value from primary store (without TTL since we can't get it from protocol)
            await self.cache_store.put(collection=collection, key=key, value=primary_store_value)
            return primary_store_value
        return None

    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        _ = await self.cache_store.delete(collection=collection, key=key)
        await self.primary_store.put(collection=collection, key=key, value=value, ttl=ttl)

    async def delete(self, collection: str, key: str) -> bool:
        deleted = await self.primary_store.delete(collection=collection, key=key)
        _ = await self.cache_store.delete(collection=collection, key=key)
        return deleted

    async def exists(self, collection: str, key: str) -> bool:
        return await self.get(collection=collection, key=key) is not None