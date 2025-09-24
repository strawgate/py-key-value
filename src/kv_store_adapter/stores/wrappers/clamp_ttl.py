from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo


class TTLClampWrapper(BaseKVStore):
    """Wrapper that enforces a maximum TTL for puts into the store."""

    def __init__(self, store: BaseKVStore, min_ttl: float, max_ttl: float, missing_ttl: float | None = None) -> None:
        """Initialize the TTL clamp wrapper.

        Args:
            store: The store to wrap.
            min_ttl: The minimum TTL for puts into the store.
            max_ttl: The maximum TTL for puts into the store.
            missing_ttl: The TTL to use for entries that do not have a TTL. Defaults to None.
        """
        self.store: BaseKVStore = store
        self.min_ttl: float = min_ttl
        self.max_ttl: float = max_ttl
        self.missing_ttl: float | None = missing_ttl

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        return await self.store.get(collection=collection, key=key)

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        if ttl is None and self.missing_ttl:
            ttl = self.missing_ttl

        if ttl and ttl < self.min_ttl:
            ttl = self.min_ttl

        if ttl and ttl > self.max_ttl:
            ttl = self.max_ttl

        await self.store.put(collection=collection, key=key, value=value, ttl=ttl)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        return await self.store.delete(collection=collection, key=key)

    @override
    async def exists(self, collection: str, key: str) -> bool:
        return await self.store.exists(collection=collection, key=key)

    @override
    async def keys(self, collection: str) -> list[str]:
        return await self.store.keys(collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        return await self.store.clear_collection(collection=collection)

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        return await self.store.ttl(collection=collection, key=key)

    @override
    async def list_collections(self) -> list[str]:
        return await self.store.list_collections()

    @override
    async def cull(self) -> None:
        await self.store.cull()
