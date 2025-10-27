from collections.abc import Sequence
from typing import Any, Generic, SupportsFloat, TypeVar, get_origin, overload

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.type_checking.bear_spray import bear_spray
from pydantic import BaseModel, ValidationError
from pydantic.type_adapter import TypeAdapter
from pydantic_core import PydanticSerializationError

from key_value.aio.protocols.key_value import AsyncKeyValue

T = TypeVar("T", bound=BaseModel | Sequence[BaseModel])


class PydanticAdapter(Generic[T]):
    """Adapter around a KVStore-compliant Store that allows type-safe persistence of Pydantic models."""

    _key_value: AsyncKeyValue
    _is_list_model: bool
    _type_adapter: TypeAdapter[T]
    _default_collection: str | None
    _raise_on_validation_error: bool

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

        self._key_value = key_value

        origin = get_origin(pydantic_model)
        self._is_list_model = origin is not None and isinstance(origin, type) and issubclass(origin, Sequence)

        self._type_adapter = TypeAdapter[T](pydantic_model)
        self._default_collection = default_collection
        self._raise_on_validation_error = raise_on_validation_error

    def _validate_model(self, value: dict[str, Any]) -> T | None:
        """Validate and deserialize a dict into the configured Pydantic model.

        This method handles both single models and list models. For list models, it expects the value
        to contain an "items" key with the list data, following the convention used by `_serialize_model`.
        If validation fails and `raise_on_validation_error` is False, returns None instead of raising.

        Args:
            value: The dict to validate and convert to a Pydantic model.

        Returns:
            The validated model instance, or None if validation fails and errors are suppressed.

        Raises:
            DeserializationError: If validation fails and `raise_on_validation_error` is True.
        """
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
        """Serialize a Pydantic model to a dict for storage.

        This method handles both single models and list models. For list models, it wraps the serialized
        list in a dict with an "items" key (e.g., {"items": [...]}) to ensure consistent dict-based storage
        format across all value types. This wrapping convention is expected by `_validate_model` during
        deserialization.

        Args:
            value: The Pydantic model instance to serialize.

        Returns:
            A dict representation of the model suitable for storage.

        Raises:
            SerializationError: If the model cannot be serialized.
        """
        try:
            if self._is_list_model:
                return {"items": self._type_adapter.dump_python(value, mode="json")}

            return self._type_adapter.dump_python(value, mode="json")  # pyright: ignore[reportAny]
        except PydanticSerializationError as e:
            msg = f"Invalid Pydantic model: {e}"
            raise SerializationError(msg) from e

    @overload
    async def get(self, key: str, *, collection: str | None = None, default: T) -> T: ...

    @overload
    async def get(self, key: str, *, collection: str | None = None, default: None = None) -> T | None: ...

    async def get(self, key: str, *, collection: str | None = None, default: T | None = None) -> T | None:
        """Get and validate a model by key.

        Args:
            key: The key to retrieve.
            collection: The collection to use. If not provided, uses the default collection.
            default: The default value to return if the key doesn't exist or validation fails.

        Returns:
            The parsed model instance if found and valid, or the default value if key doesn't exist or validation fails.

        Raises:
            DeserializationError if the stored data cannot be validated as the model and the PydanticAdapter is configured to
            raise on validation error.

        Note:
            When raise_on_validation_error=False and validation fails, returns the default value (which may be None).
            When raise_on_validation_error=True and validation fails, raises DeserializationError.
        """
        collection = collection or self._default_collection

        if value := await self._key_value.get(key=key, collection=collection):
            validated = self._validate_model(value=value)
            if validated is not None:
                return validated

        return default

    @overload
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None, default: T) -> list[T]: ...

    @overload
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None, default: None = None) -> list[T | None]: ...

    async def get_many(self, keys: Sequence[str], *, collection: str | None = None, default: T | None = None) -> list[T] | list[T | None]:
        """Batch get and validate models by keys, preserving order.

        Args:
            keys: The list of keys to retrieve.
            collection: The collection to use. If not provided, uses the default collection.
            default: The default value to return for keys that don't exist or fail validation.

        Returns:
            A list of parsed model instances, with default values for missing keys or validation failures.

        Raises:
            DeserializationError if the stored data cannot be validated as the model and the PydanticAdapter is configured to
            raise on validation error.

        Note:
            When raise_on_validation_error=False and validation fails for any key, that position in the returned list
            will contain the default value (which may be None). The method returns a complete list matching the order
            and length of the input keys, with defaults substituted for missing or invalid entries.
        """
        collection = collection or self._default_collection

        values: list[dict[str, Any] | None] = await self._key_value.get_many(keys=keys, collection=collection)

        result: list[T | None] = []
        for value in values:
            if value is None:
                result.append(default)
            else:
                validated = self._validate_model(value=value)
                result.append(validated if validated is not None else default)
        return result

    async def put(self, key: str, value: T, *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        """Serialize and store a model.

        Propagates SerializationError if the model cannot be serialized.
        """
        collection = collection or self._default_collection

        value_dict: dict[str, Any] = self._serialize_model(value=value)

        await self._key_value.put(key=key, value=value_dict, collection=collection, ttl=ttl)

    async def put_many(
        self, keys: Sequence[str], values: Sequence[T], *, collection: str | None = None, ttl: SupportsFloat | None = None
    ) -> None:
        """Serialize and store multiple models, preserving order alignment with keys."""
        collection = collection or self._default_collection

        value_dicts: list[dict[str, Any]] = [self._serialize_model(value=value) for value in values]

        await self._key_value.put_many(keys=keys, values=value_dicts, collection=collection, ttl=ttl)

    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        """Delete a model by key. Returns True if a value was deleted, else False."""
        collection = collection or self._default_collection

        return await self._key_value.delete(key=key, collection=collection)

    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        """Delete multiple models by key. Returns the count of deleted entries."""
        collection = collection or self._default_collection

        return await self._key_value.delete_many(keys=keys, collection=collection)

    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[T | None, float | None]:
        """Get a model and its TTL seconds if present.

        Args:
            key: The key to retrieve.
            collection: The collection to use. If not provided, uses the default collection.

        Returns:
            A tuple of (model, ttl_seconds). Returns (None, None) if the key is missing or validation fails.

        Note:
            When validation fails and raise_on_validation_error=False, returns (None, None) even if TTL data exists.
            When validation fails and raise_on_validation_error=True, raises DeserializationError.
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

    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[T | None, float | None]]:
        """Batch get models with TTLs. Each element is (model|None, ttl_seconds|None)."""
        collection = collection or self._default_collection

        entries: list[tuple[dict[str, Any] | None, float | None]] = await self._key_value.ttl_many(keys=keys, collection=collection)

        return [(self._validate_model(value=entry) if entry else None, ttl_info) for entry, ttl_info in entries]
