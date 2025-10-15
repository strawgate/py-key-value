from collections.abc import Sequence
from typing import Any, SupportsFloat

from key_value.shared.errors import EntryTooLargeError
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class LimitSizeWrapper(BaseWrapper):
    """Wrapper that limits the size of entries stored in the cache.

    This wrapper checks the serialized size of values before storing them.
    It can either raise an error or silently ignore entries that exceed the size limit.
    """

    def __init__(self, key_value: AsyncKeyValue, max_size: int, *, raise_on_error: bool = True) -> None:
        """Initialize the limit size wrapper.

        Args:
            key_value: The store to wrap.
            max_size: The maximum size (in bytes) allowed for each entry.
            raise_on_error: If True, raises EntryTooLargeError when an entry exceeds max_size.
                           If False, silently ignores entries that are too large.
        """
        self.key_value: AsyncKeyValue = key_value
        self.max_size: int = max_size
        self.raise_on_error: bool = raise_on_error

        super().__init__()

    def _check_size(self, value: dict[str, Any], *, collection: str | None = None, key: str | None = None) -> bool:
        """Check if a value exceeds the maximum size.

        Args:
            value: The value to check.
            collection: The collection name (for error messages).
            key: The key name (for error messages).

        Returns:
            True if the value is within the size limit, False otherwise.

        Raises:
            EntryTooLargeError: If raise_on_error is True and the value exceeds max_size.
        """
        # Create a ManagedEntry to get the JSON representation
        managed_entry = ManagedEntry(value=value)
        json_str = managed_entry.to_json()
        size = len(json_str.encode("utf-8"))

        if size > self.max_size:
            if self.raise_on_error:
                raise EntryTooLargeError(size=size, max_size=self.max_size, collection=collection, key=key)
            return False

        return True

    @override
    async def put(
        self, key: str, value: dict[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None
    ) -> None:
        if self._check_size(value=value, collection=collection, key=key):
            await self.key_value.put(collection=collection, key=key, value=value, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[dict[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | SupportsFloat | None = None,
    ) -> None:
        # Filter out values that exceed the size limit
        filtered_keys: list[str] = []
        filtered_values: list[dict[str, Any]] = []
        filtered_ttls: list[SupportsFloat | None] | SupportsFloat | None = None

        if isinstance(ttl, Sequence):
            filtered_ttls = []

        for i, (k, v) in enumerate(zip(keys, values, strict=True)):
            if self._check_size(value=v, collection=collection, key=k):
                filtered_keys.append(k)
                filtered_values.append(v)
                if isinstance(ttl, Sequence):
                    filtered_ttls.append(ttl[i])  # type: ignore[union-attr]

        if filtered_keys:
            # Use the original ttl if it's not a sequence
            if not isinstance(ttl, Sequence):
                filtered_ttls = ttl

            await self.key_value.put_many(
                keys=filtered_keys, values=filtered_values, collection=collection, ttl=filtered_ttls
            )
