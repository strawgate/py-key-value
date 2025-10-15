from collections.abc import Sequence
from typing import Any, Generic, SupportsFloat, TypeVar, get_origin

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.type_checking.bear_spray import bear_spray
from pydantic import BaseModel, ValidationError
from pydantic.type_adapter import TypeAdapter
from pydantic_core import PydanticSerializationError

from key_value.aio.protocols.key_value import AsyncKeyValue

T = TypeVar("T", bound=BaseModel | Sequence[BaseModel])


class PydanticAdapter(Generic[T]):
    """Adapter around a KVStore-compliant Store that allows type-safe persistence of Pydantic models."""

    # Beartype doesn't like our `type[T] includes a bound on Sequence[...] as the subscript is not checkable at runtime
    # For just the next 20 or so lines we are no longer bear bros but have no fear, we will be back soon!
    @bear_spray
    def __init__(
        self,
        key_value: AsyncKeyValue,
        pydantic_model: type[T],
        default_collection: str | None = None,
        raise_on_validation_error: bool = False,
    ) -> None:
        """Create a new PydanticAdapter.

        Args:
            key_value: The KVStore to use.
            pydantic_model: The Pydantic model to use.
            default_collection: The default collection to use.
            raise_on_validation_error: Whether to raise a ValidationError if the model is invalid.
        """

        self._key_value: AsyncKeyValue = key_value

        origin = get_origin(pydantic_model)
        self._is_list_model: bool = origin is not None and issubclass(origin, Sequence)

        self._type_adapter = TypeAdapter[T](pydantic_model)
        self._default_collection: str | None = default_collection
        self._raise_on_validation_error: bool = raise_on_validation_error

    def _validate_model(self, value: dict[str, Any]) -> T | None:
        try:
            if self._is_list_model:
                return self._type_adapter.validate_python(value.get("items", []))

            return self._type_adapter.validate_python(value)
        except ValidationError as e:
            if self._raise_on_validation_error:
                msg = f"Invalid Pydantic model: {value}"
                raise DeserializationError(msg) from e
            return None

    def _serialize_model(self, value: T) -> dict[str, Any]:
        try:
            if self._is_list_model:
                return {"items": self._type_adapter.dump_python(value, mode="json")}

            return self._type_adapter.dump_python(value, mode="json")
        except PydanticSerializationError as e:
            msg = f"Invalid Pydantic model: {e}"
            raise SerializationError(msg) from e

    async def get(self, key: str, *, collection: str | None = None) -> T | None:
        """Get and validate a model by key.

        Returns:
            The parsed model instance, or None if not present.

        Raises:
            DeserializationError if the stored data cannot be validated as the model and the PydanticAdapter is configured to
            raise on validation error.
        """
        collection = collection or self._default_collection

        if value := await self._key_value.get(key=key, collection=collection):
            return self._validate_model(value=value)

        return None

    async def get_many(self, keys: list[str], *, collection: str | None = None) -> list[T | None]:
        """Batch get and validate models by keys, preserving order.

        Returns:
            A list of parsed model instances, or None if missing.

        Raises:
            DeserializationError if the stored data cannot be validated as the model and the PydanticAdapter is configured to
            raise on validation error.
        """
        collection = collection or self._default_collection

        values: list[dict[str, Any] | None] = await self._key_value.get_many(keys=keys, collection=collection)

        return [self._validate_model(value=value) if value else None for value in values]

    async def put(self, key: str, value: T, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        """Serialize and store a model.

        Propagates SerializationError if the model cannot be serialized.
        """
        collection = collection or self._default_collection

        value_dict: dict[str, Any] = self._serialize_model(value=value)

        await self._key_value.put(key=key, value=value_dict, collection=collection, ttl=ttl)

    async def put_many(
        self, keys: list[str], values: Sequence[T], *, collection: str | None = None, ttl: Sequence[SupportsFloat | None] | None = None
    ) -> None:
        """Serialize and store multiple models, preserving order alignment with keys."""
        collection = collection or self._default_collection

        value_dicts: list[dict[str, Any]] = [self._serialize_model(value=value) for value in values]

        await self._key_value.put_many(keys=keys, values=value_dicts, collection=collection, ttl=ttl)

    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        """Delete a model by key. Returns True if a value was deleted, else False."""
        collection = collection or self._default_collection

        return await self._key_value.delete(key=key, collection=collection)

    async def delete_many(self, keys: list[str], *, collection: str | None = None) -> int:
        """Delete multiple models by key. Returns the count of deleted entries."""
        collection = collection or self._default_collection

        return await self._key_value.delete_many(keys=keys, collection=collection)

    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[T | None, float | None]:
        """Get a model and its TTL seconds if present.

        Returns (model, ttl_seconds) or (None, None) if missing.
        """
        collection = collection or self._default_collection

        entry: dict[str, Any] | None
        ttl_info: float | None

        entry, ttl_info = await self._key_value.ttl(key=key, collection=collection)

        if entry is None:
            return (None, None)

        if validated_model := self._validate_model(value=entry):
            return (validated_model, ttl_info)

        return (None, None)

    async def ttl_many(self, keys: list[str], *, collection: str | None = None) -> list[tuple[T | None, float | None]]:
        """Batch get models with TTLs. Each element is (model|None, ttl_seconds|None)."""
        collection = collection or self._default_collection

        entries: list[tuple[dict[str, Any] | None, float | None]] = await self._key_value.ttl_many(keys=keys, collection=collection)

        return [(self._validate_model(value=entry) if entry else None, ttl_info) for entry, ttl_info in entries]
