from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_key, unprefix_key
from kv_store_adapter.types import TTLInfo


class SingleCollectionWrapper(BaseKVStore):
    """Wrapper that forces all requests into a single collection, prefixes the keys with the original collection name.

    The single collection wrapper does not support collection operations."""

    def __init__(self, store: BaseKVStore, collection: str, prefix_separator: str | None = None) -> None:
        self.collection: str = collection
        self.prefix_separator: str = prefix_separator or DEFAULT_PREFIX_SEPARATOR
        self.store: BaseKVStore = store

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.get(collection=self.collection, key=prefixed_key)

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        await self.store.put(collection=self.collection, key=prefixed_key, value=value, ttl=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.delete(collection=self.collection, key=prefixed_key)

    @override
    async def exists(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        return await self.store.exists(collection=self.collection, key=prefixed_key)

    @override
    async def keys(self, collection: str) -> list[str]:
        keys: list[str] = await self.store.keys(collection=collection)
        return [unprefix_key(key=key, separator=self.prefix_separator) for key in keys]

    @override
    async def clear_collection(self, collection: str) -> int:
        msg = "Clearing a collection is not supported for SingleCollectionWrapper"
        raise NotImplementedError(msg)

        # return await self.store.clear_collection(collection=self.collection)

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        prefixed_key: str = prefix_key(key=key, prefix=collection, separator=self.prefix_separator)
        ttl: TTLInfo | None = await self.store.ttl(collection=self.collection, key=prefixed_key)
        if ttl:
            ttl.collection = collection
            ttl.key = key
        return ttl

    @override
    async def list_collections(self) -> list[str]:
        msg = "Listing collections is not supported for SingleCollectionWrapper"
        raise NotImplementedError(msg)

    @override
    async def cull(self) -> None:
        await self.store.cull()
