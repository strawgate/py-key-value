from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_key, unprefix_key
from kv_store_adapter.types import TTLInfo


class PrefixKeyWrapper(BaseKVStore):
    """Wrapper that prefixes all keys with a given prefix."""

    def __init__(self, store: BaseKVStore, prefix: str, separator: str | None = None) -> None:
        """Initialize the prefix key wrapper.

        Args:
            store: The store to wrap.
            prefix: The prefix to add to all keys.
            separator: The separator to use between the prefix and the key. Defaults to "__".
        """
        self.store: BaseKVStore = store
        self.prefix: str = prefix
        self.separator: str = separator or DEFAULT_PREFIX_SEPARATOR

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.get(collection=collection, key=prefixed_key)

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        await self.store.put(collection=collection, key=prefixed_key, value=value, ttl=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.delete(collection=collection, key=prefixed_key)

    @override
    async def exists(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.exists(collection=collection, key=prefixed_key)

    @override
    async def keys(self, collection: str) -> list[str]:
        keys: list[str] = await self.store.keys(collection=collection)
        return [unprefix_key(key=key, separator=self.separator) for key in keys]

    @override
    async def clear_collection(self, collection: str) -> int:
        return await self.store.clear_collection(collection=collection)

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        ttl_info: TTLInfo | None = await self.store.ttl(collection=collection, key=prefixed_key)
        if ttl_info:
            ttl_info.collection = collection
            ttl_info.key = key
        return ttl_info

    @override
    async def list_collections(self) -> list[str]:
        return await self.store.list_collections()

    @override
    async def cull(self) -> None:
        await self.store.cull()
