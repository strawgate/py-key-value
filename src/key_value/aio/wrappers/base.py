from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

from typing_extensions import override

from key_value.aio._utils.beartype import bear_enforce
from key_value.aio.protocols.key_value import AsyncKeyValue, AsyncPutIfAbsentProtocol


class BaseWrapper(AsyncKeyValue, AsyncPutIfAbsentProtocol):
    """A base wrapper for KVStore implementations that passes through to the underlying store.

    This class implements the passthrough pattern where all operations are delegated to the wrapped
    key-value store without modification. It serves as a foundation for creating custom wrappers that
    need to intercept, modify, or enhance specific operations while passing through others unchanged.

    To create a custom wrapper, subclass this class and override only the methods you need to customize.
    All other operations will automatically pass through to the underlying store.

    Example:
        class LoggingWrapper(BaseWrapper):
            async def get(self, key: str, *, collection: str | None = None):
                logger.info(f"Getting key: {key}")
                return await super().get(key, collection=collection)

    Attributes:
        key_value: The underlying AsyncKeyValue store that operations are delegated to.
    """

    key_value: AsyncKeyValue

    @bear_enforce
    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        return await self.key_value.get(collection=collection, key=key)

    @bear_enforce
    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        return await self.key_value.get_many(collection=collection, keys=keys)

    @bear_enforce
    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        return await self.key_value.ttl(collection=collection, key=key)

    @bear_enforce
    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self.key_value.ttl_many(collection=collection, keys=keys)

    @bear_enforce
    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        return await self.key_value.put(collection=collection, key=key, value=value, ttl=ttl)

    @bear_enforce
    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        return await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=ttl)

    @bear_enforce
    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        return await self.key_value.delete(collection=collection, key=key)

    @bear_enforce
    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        return await self.key_value.delete_many(keys=keys, collection=collection)

    @bear_enforce
    @override
    async def put_if_absent(
        self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None
    ) -> bool:
        """Forward to the wrapped store, if it supports atomic conditional writes.

        `isinstance(wrapper, AsyncPutIfAbsentProtocol)` can't reflect whether the *wrapped* store
        supports this -- Python's runtime-checkable protocols only check for the method's presence
        on the wrapper's own class, not the object it delegates to -- so this raises clearly instead
        of letting an isinstance-guarded caller believe the write was atomic when it wasn't attempted.
        """
        if not isinstance(self.key_value, AsyncPutIfAbsentProtocol):
            msg = f"{type(self.key_value).__name__} does not support put_if_absent"
            raise NotImplementedError(msg)
        return await self.key_value.put_if_absent(key=key, value=value, collection=collection, ttl=ttl)
