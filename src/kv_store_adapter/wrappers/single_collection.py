from typing import Any

from kv_store_adapter.stores.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_key
from kv_store_adapter.types import KVStoreProtocol


class SingleCollectionWrapper:
    """Wrapper that forces all requests into a single collection, prefixes the keys with the original collection name.

    The single collection wrapper does not support management operations."""

    def __init__(self, store: KVStoreProtocol, collection: str, prefix_separator: str | None = None) -> None:
        self.collection: str = collection
        self.prefix_separator: str = prefix_separator or DEFAULT_PREFIX_SEPARATOR
        self.store: KVStoreProtocol = store

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.get(collection=self.collection, key=prefixed_key)

    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        await self.store.put(collection=self.collection, key=prefixed_key, value=value, ttl=ttl)

    async def delete(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.delete(collection=self.collection, key=prefixed_key)

    async def exists(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.exists(collection=self.collection, key=prefixed_key)