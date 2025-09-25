from typing import Any

from kv_store_adapter.stores.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_key
from kv_store_adapter.types import KVStoreProtocol


class PrefixKeyWrapper:
    """Wrapper that prefixes all keys with a given prefix."""

    def __init__(self, store: KVStoreProtocol, prefix: str, separator: str | None = None) -> None:
        """Initialize the prefix key wrapper.

        Args:
            store: The store to wrap.
            prefix: The prefix to add to all keys.
            separator: The separator to use between the prefix and the key. Defaults to "__".
        """
        self.store: KVStoreProtocol = store
        self.prefix: str = prefix
        self.separator: str = separator or DEFAULT_PREFIX_SEPARATOR

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.get(collection=collection, key=prefixed_key)

    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        await self.store.put(collection=collection, key=prefixed_key, value=value, ttl=ttl)

    async def delete(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.delete(collection=collection, key=prefixed_key)

    async def exists(self, collection: str, key: str) -> bool:
        prefixed_key: str = prefix_key(key=key, prefix=self.prefix, separator=self.separator)
        return await self.store.exists(collection=collection, key=prefixed_key)