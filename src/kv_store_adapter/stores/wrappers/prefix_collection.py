from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_collection, unprefix_collection
from kv_store_adapter.types import TTLInfo


class PrefixCollectionWrapper(BaseKVStore):
    """Wrapper that prefixes all collections with a given prefix."""

    def __init__(self, store: BaseKVStore, prefix: str, separator: str | None = None) -> None:
        """Initialize the prefix collection wrapper.

        Args:
            store: The store to wrap.
            prefix: The prefix to add to all collections.
            separator: The separator to use between the prefix and the collection. Defaults to "__".
        """
        self.store: BaseKVStore = store
        self.prefix: str = prefix
        self.separator: str = separator or DEFAULT_PREFIX_SEPARATOR

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        return await self.store.get(collection=prefixed_collection, key=key)

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        await self.store.put(collection=prefixed_collection, key=key, value=value, ttl=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        return await self.store.delete(collection=prefixed_collection, key=key)

    @override
    async def exists(self, collection: str, key: str) -> bool:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        return await self.store.exists(collection=prefixed_collection, key=key)

    @override
    async def keys(self, collection: str) -> list[str]:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        return await self.store.keys(collection=prefixed_collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        return await self.store.clear_collection(collection=prefixed_collection)

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        prefixed_collection: str = prefix_collection(collection=collection, prefix=self.prefix, separator=self.separator)
        ttl_info: TTLInfo | None = await self.store.ttl(collection=prefixed_collection, key=key)
        if ttl_info:
            ttl_info.collection = collection
            ttl_info.key = key
        return ttl_info

    @override
    async def list_collections(self) -> list[str]:
        collections: list[str] = await self.store.list_collections()

        return [
            unprefix_collection(collection=collection, separator=self.separator)
            for collection in collections
            if collection.startswith(self.prefix)
        ]

    @override
    async def cull(self) -> None:
        await self.store.cull()
