from collections.abc import Sequence
from typing import Any, overload

from key_value.shared.utils.time_to_live import validate_ttl
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class TTLClampWrapper(BaseWrapper):
    """Wrapper that enforces a maximum TTL for puts into the store."""

    def __init__(self, store: AsyncKeyValue, min_ttl: float, max_ttl: float, missing_ttl: float | None = None) -> None:
        """Initialize the TTL clamp wrapper.

        Args:
            store: The store to wrap.
            min_ttl: The minimum TTL for puts into the store.
            max_ttl: The maximum TTL for puts into the store.
            missing_ttl: The TTL to use for entries that do not have a TTL. Defaults to None.
        """
        self.store: AsyncKeyValue = store
        self.min_ttl: float = min_ttl
        self.max_ttl: float = max_ttl
        self.missing_ttl: float | None = missing_ttl

        super().__init__()

    @overload
    def _ttl_clamp(self, ttl: float) -> float: ...

    @overload
    def _ttl_clamp(self, ttl: float | None) -> float | None: ...

    def _ttl_clamp(self, ttl: float | None) -> float | None:
        if ttl is None:
            return self.missing_ttl

        ttl = validate_ttl(t=ttl)

        return max(self.min_ttl, min(ttl, self.max_ttl))

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: float | None = None) -> None:
        await self.store.put(collection=collection, key=key, value=value, ttl=self._ttl_clamp(ttl=ttl))

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[float | None] | float | None = None,
    ) -> None:
        if isinstance(ttl, (float, int)):
            ttl = self._ttl_clamp(ttl=ttl)

        if isinstance(ttl, Sequence):
            ttl = [self._ttl_clamp(ttl=t) for t in ttl]

        await self.store.put_many(keys=keys, values=values, collection=collection, ttl=ttl)
