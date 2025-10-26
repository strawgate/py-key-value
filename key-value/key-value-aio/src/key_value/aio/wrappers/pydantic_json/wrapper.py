from collections.abc import Mapping, Sequence
from typing import Any, SupportsFloat

from pydantic import TypeAdapter
from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.base import BaseWrapper


class PydanticJsonWrapper(BaseWrapper):
    """Wrapper that ensures all values are JSON-serializable using Pydantic's TypeAdapter.

    This wrapper validates and converts incoming values to JSON-compatible Python objects
    before storing them. This ensures that all stored values can be safely serialized to JSON,
    which is useful when working with stores that require JSON-serializable data or when
    you want to guarantee that data can be exported to JSON format.

    Example:
        >>> from datetime import datetime
        >>> wrapped = PydanticJsonWrapper(key_value=memory_store)
        >>> # This will convert datetime objects to strings, etc.
        >>> await wrapped.put("key", {"timestamp": datetime.now(), "count": 42})
    """

    def __init__(self, key_value: AsyncKeyValue) -> None:
        """Initialize the Pydantic JSON wrapper.

        Args:
            key_value: The underlying key-value store to wrap.
        """
        self.key_value: AsyncKeyValue = key_value
        self._json_adapter: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])

        super().__init__()

    def _to_json_safe(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Convert a value to JSON-safe format using Pydantic.

        Args:
            value: The value to convert.

        Returns:
            A JSON-serializable dictionary.
        """
        return self._json_adapter.dump_python(value, mode="json")

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        json_safe_value = self._to_json_safe(value)
        await self.key_value.put(key=key, value=json_safe_value, collection=collection, ttl=ttl)

    @override
    async def put_many(
        self,
        keys: list[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: Sequence[SupportsFloat | None] | None = None,
    ) -> None:
        json_safe_values = [self._to_json_safe(value) for value in values]
        await self.key_value.put_many(keys=keys, values=json_safe_values, collection=collection, ttl=ttl)
