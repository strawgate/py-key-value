from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo


class NullStore(BaseKVStore):
    """Null object pattern store that accepts all operations but stores nothing."""

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        return None

    @override
    async def put(
        self,
        collection: str,
        key: str,
        value: dict[str, Any],
        *,
        ttl: float | None = None,
    ) -> None:
        pass

    @override
    async def delete(self, collection: str, key: str) -> bool:
        return False

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        return None

    @override
    async def keys(self, collection: str) -> list[str]:
        return []

    @override
    async def clear_collection(self, collection: str) -> int:
        return 0

    @override
    async def list_collections(self) -> list[str]:
        return []

    @override
    async def cull(self) -> None:
        pass

    @override
    async def exists(self, collection: str, key: str) -> bool:
        return False
