from collections.abc import Sequence
from typing import Any, overload

from typing_extensions import override

from kv_store_adapter.types import KVStore
from kv_store_adapter.wrappers.base import BaseWrapper


class TTLClampWrapper(BaseWrapper):
    """Wrapper that enforces a maximum TTL for puts into the store."""

    def __init__(self, store: KVStore, min_ttl: float, max_ttl: float, missing_ttl: float | None = None) -> None:
        """Initialize the TTL clamp wrapper.

        Args:
            store: The store to wrap.
            min_ttl: The minimum TTL for puts into the store.
            max_ttl: The maximum TTL for puts into the store.
            missing_ttl: The TTL to use for entries that do not have a TTL. Defaults to None.
        """
        self.store: KVStore = store
        self.min_ttl: float = min_ttl
        self.max_ttl: float = max_ttl
        self.missing_ttl: float | None = missing_ttl

        super().__init__()

    @overload
    def _clamp_ttl(self, ttl: float) -> float: ...

    @overload
    def _clamp_ttl(self, ttl: float | None) -> float | None: ...

    def _clamp_ttl(self, ttl: float | None) -> float | None:
        if ttl is None:
            return self.missing_ttl

        return max(self.min_ttl, min(ttl, self.max_ttl))

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: float | None = None) -> None:
        await self.store.put(collection=collection, key=key, value=value, ttl=self._clamp_ttl(ttl=ttl))

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[float | None] | float | None = None,
    ) -> None:
        clamped_ttl: Sequence[float | None] | float | None = None

        if isinstance(ttl, Sequence):
            clamped_ttl = [self._clamp_ttl(ttl=t) for t in ttl]
        elif isinstance(ttl, float):
            clamped_ttl = self._clamp_ttl(ttl=ttl)

        await self.store.put_many(keys=keys, values=values, collection=collection, ttl=clamped_ttl)
