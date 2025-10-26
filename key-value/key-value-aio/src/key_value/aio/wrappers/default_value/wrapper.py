from collections.abc import Mapping
from typing import Any

from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class DefaultValueWrapper(BaseWrapper):
    """Wrapper that returns a default value when a key doesn't exist.

    This wrapper provides a convenient way to handle missing keys by returning
    a default value instead of None, similar to Python's dict.get(key, default)
    or collections.defaultdict behavior.

    Example:
        >>> wrapped = DefaultValueWrapper(
        ...     key_value=memory_store,
        ...     default_value={"status": "not_found"}
        ... )
        >>> result = await wrapped.get("missing_key")
        >>> # Returns {"status": "not_found"} instead of None
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        default_value: Mapping[str, Any],
    ) -> None:
        """Initialize the default value wrapper.

        Args:
            key_value: The underlying key-value store to wrap.
            default_value: The default value to return when a key doesn't exist.
        """
        self.key_value: AsyncKeyValue = key_value
        self.default_value: Mapping[str, Any] = default_value

        super().__init__()

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        result = await self.key_value.get(key=key, collection=collection)
        return result if result is not None else dict(self.default_value)

    @override
    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        results = await self.key_value.get_many(keys=keys, collection=collection)
        return [result if result is not None else dict(self.default_value) for result in results]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        value, ttl_seconds = await self.key_value.ttl(key=key, collection=collection)
        if value is None:
            return (dict(self.default_value), None)
        return (value, ttl_seconds)

    @override
    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        return [(value if value is not None else dict(self.default_value), ttl_seconds) for value, ttl_seconds in results]
