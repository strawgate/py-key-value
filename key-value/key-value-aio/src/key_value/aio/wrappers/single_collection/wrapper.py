from collections.abc import Sequence
from typing import Any

from key_value.shared.utils.compound import DEFAULT_PREFIX_SEPARATOR, prefix_key, unprefix_key
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.base import DEFAULT_COLLECTION_NAME
from key_value.aio.wrappers.base import BaseWrapper


class SingleCollectionWrapper(BaseWrapper):
    """A wrapper that stores all collections within a single backing collection via key prefixing."""

    def __init__(
        self, store: AsyncKeyValue, single_collection: str, default_collection: str | None = None, separator: str | None = None
    ) -> None:
        """Initialize the prefix collections wrapper.

        Args:
            store: The store to wrap.
            single_collection: The single collection to use to store all collections.
            default_collection: The default collection to use if no collection is provided.
        """
        self.store: AsyncKeyValue = store
        self.single_collection: str = single_collection
        self.default_collection: str = default_collection or DEFAULT_COLLECTION_NAME
        self.separator: str = separator or DEFAULT_PREFIX_SEPARATOR
        super().__init__()

    def _prefix_key(self, key: str, collection: str | None = None) -> str:
        collection_to_use = collection or self.default_collection
        return prefix_key(prefix=collection_to_use, key=key, separator=self.separator)

    def _unprefix_key(self, key: str) -> str:
        return unprefix_key(prefix=self.single_collection, key=key, separator=self.separator)

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        new_key: str = self._prefix_key(key=key, collection=collection)
        return await self.store.get(key=new_key, collection=self.single_collection)

    @override
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        new_keys: list[str] = [self._prefix_key(key=key, collection=collection) for key in keys]
        return await self.store.get_many(keys=new_keys, collection=self.single_collection)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        new_key: str = self._prefix_key(key=key, collection=collection)
        return await self.store.ttl(key=new_key, collection=self.single_collection)

    @override
    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        new_keys: list[str] = [self._prefix_key(key=key, collection=collection) for key in keys]
        return await self.store.ttl_many(keys=new_keys, collection=self.single_collection)

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: float | None = None) -> None:
        new_key: str = self._prefix_key(key=key, collection=collection)
        return await self.store.put(key=new_key, value=value, collection=self.single_collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[float | None] | float | None = None,
    ) -> None:
        new_keys: list[str] = [self._prefix_key(key=key, collection=collection) for key in keys]
        return await self.store.put_many(keys=new_keys, values=values, collection=self.single_collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        new_key: str = self._prefix_key(key=key, collection=collection)
        return await self.store.delete(key=new_key, collection=self.single_collection)

    @override
    async def delete_many(self, keys: list[str], *, collection: str | None = None) -> int:
        new_keys: list[str] = [self._prefix_key(key=key, collection=collection) for key in keys]
        return await self.store.delete_many(keys=new_keys, collection=self.single_collection)
