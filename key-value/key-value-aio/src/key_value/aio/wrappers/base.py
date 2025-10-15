from collections.abc import Sequence
from typing import Any, SupportsFloat

from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue


class BaseWrapper(AsyncKeyValue):
    """A base wrapper for KVStore implementations that passes through to the underlying store."""

    key_value: AsyncKeyValue

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        return await self.key_value.get(collection=collection, key=key)

    @override
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        return await self.key_value.get_many(collection=collection, keys=keys)

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        return await self.key_value.ttl(collection=collection, key=key)

    @override
    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self.key_value.ttl_many(collection=collection, keys=keys)

    @override
    async def put(self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        return await self.key_value.put(collection=collection, key=key, value=value, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | None = None,
    ) -> None:
        return await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        return await self.key_value.delete(collection=collection, key=key)

    @override
    async def delete_many(self, keys: list[str], *, collection: str | None = None) -> int:
        return await self.key_value.delete_many(keys=keys, collection=collection)
