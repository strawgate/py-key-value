from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

from pydantic import TypeAdapter
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class PydanticJsonWrapper(BaseWrapper):
    """A wrapper that ensures all values are JSON-serializable using Pydantic's TypeAdapter.

    This wrapper automatically converts values to JSON-safe formats before storage,
    ensuring compatibility with stores that require JSON-serializable data.
    """

    def __init__(self, key_value: AsyncKeyValue) -> None:
        """Initialize the PydanticJsonWrapper.

        Args:
            key_value: The underlying key-value store to wrap.
        """
        self.key_value = key_value
        self._adapter: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])

    def _to_json_safe(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Convert a value to a JSON-safe format using Pydantic.

        Args:
            value: The value to convert.

        Returns:
            A JSON-safe dictionary.
        """
        return self._adapter.dump_python(value, mode="json")  # type: ignore[return-value]

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        """Store a value after converting it to JSON-safe format.

        Args:
            key: The key to store.
            value: The value to store (will be converted to JSON-safe format).
            collection: The collection to use.
            ttl: The time-to-live in seconds.
        """
        json_safe_value = self._to_json_safe(value)
        await self.key_value.put(key=key, value=json_safe_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | None = None,
    ) -> None:
        """Store multiple values after converting them to JSON-safe format.

        Args:
            keys: The keys to store.
            values: The values to store (will be converted to JSON-safe format).
            collection: The collection to use.
            ttl: The time-to-live values in seconds.
        """
        json_safe_values = [self._to_json_safe(value) for value in values]
        await self.key_value.put_many(keys=keys, values=json_safe_values, collection=collection, ttl=ttl)
