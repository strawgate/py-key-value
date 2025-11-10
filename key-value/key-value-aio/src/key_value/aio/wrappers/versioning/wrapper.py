from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper

# Special keys used to store version information
_VERSION_KEY = "__version__"
_VERSIONED_DATA_KEY = "__versioned_data__"


class VersioningWrapper(BaseWrapper):
    """Wrapper that adds version tagging to values for schema evolution and cache invalidation.

    This wrapper automatically tags all stored values with a version identifier. When retrieving
    values, it checks the version and returns None for values with mismatched versions, effectively
    auto-invalidating old cache entries.

    This is useful for:
    - Schema evolution: When your data structure changes, old cached values are automatically invalidated
    - Deployment coordination: Different versions of your application can coexist without sharing incompatible cached data
    - Safe cache invalidation: Increment the version to invalidate all cached entries without manual cleanup

    The versioned format looks like:
    {
        "__version__": "v1.2.0",
        "__versioned_data__": {
            "actual": "user",
            "data": "here"
        }
    }

    Example:
        # Version 1 of your application
        store_v1 = VersioningWrapper(key_value=store, version="v1")
        await store_v1.put(key="user:123", value={"name": "John", "email": "john@example.com"})

        # Version 2 changes the schema (adds "age" field)
        store_v2 = VersioningWrapper(key_value=store, version="v2")
        result = await store_v2.get(key="user:123")
        # Returns None because version mismatch, forcing reload with new schema
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        version: str | int,
    ) -> None:
        """Initialize the versioning wrapper.

        Args:
            key_value: The store to wrap.
            version: The version identifier to tag values with. Can be string (e.g., "v1.2.0") or int (e.g., 1).
        """
        self.key_value: AsyncKeyValue = key_value
        self.version: str | int = version

        super().__init__()

    def _wrap_value(self, value: dict[str, Any]) -> dict[str, Any]:
        """Wrap a value with version information."""
        # If already properly versioned, don't double-wrap
        if _VERSION_KEY in value and _VERSIONED_DATA_KEY in value:
            return value

        return {_VERSION_KEY: self.version, _VERSIONED_DATA_KEY: value}

    def _unwrap_value(self, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """Unwrap a versioned value, returning None if version mismatch."""
        if value is None:
            return None

        # Not versioned, return as-is
        if _VERSION_KEY not in value:
            return value

        # Check version match
        if value[_VERSION_KEY] != self.version:
            # Version mismatch - auto-invalidate by returning None
            return None

        # Extract the actual data (must be present in properly wrapped data)
        if _VERSIONED_DATA_KEY not in value:
            # Malformed versioned data - treat as corruption
            return None
        return value[_VERSIONED_DATA_KEY]

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        value = await self.key_value.get(key=key, collection=collection)
        return self._unwrap_value(value)

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        values = await self.key_value.get_many(keys=keys, collection=collection)
        return [self._unwrap_value(value) for value in values]

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        value, ttl = await self.key_value.ttl(key=key, collection=collection)
        unwrapped = self._unwrap_value(value)
        # If version mismatch, return None for TTL as well
        return unwrapped, ttl if unwrapped is not None else None

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        results = await self.key_value.ttl_many(keys=keys, collection=collection)
        unwrapped = [(self._unwrap_value(value), ttl) for value, ttl in results]
        return [(value, ttl if value is not None else None) for value, ttl in unwrapped]

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        wrapped_value = self._wrap_value(dict(value))
        return await self.key_value.put(key=key, value=wrapped_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        wrapped_values = [self._wrap_value(dict(value)) for value in values]
        return await self.key_value.put_many(keys=keys, values=wrapped_values, collection=collection, ttl=ttl)
