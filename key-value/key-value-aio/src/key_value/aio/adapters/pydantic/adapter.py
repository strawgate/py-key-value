from typing import Any, TypeVar, get_origin

from key_value.shared.type_checking.bear_spray import bear_spray
from pydantic import BaseModel
from pydantic.type_adapter import TypeAdapter
from typing_extensions import TypeForm

from key_value.aio.adapters.pydantic.base import BasePydanticAdapter
from key_value.aio.protocols.key_value import AsyncKeyValue

T = TypeVar("T")


class PydanticAdapter(BasePydanticAdapter[T]):
    """Adapter for persisting any pydantic-serializable type.

    This is the "less safe" adapter that accepts any Python type that Pydantic can serialize.
    Unlike BaseModelAdapter (which is constrained to BaseModel types), this adapter can handle:
    - Pydantic BaseModel instances
    - Dataclasses (standard and Pydantic)
    - TypedDict
    - Primitive types (int, str, float, bool, etc.)
    - Collection types (list, dict, set, tuple, etc.)
    - Datetime and other common types

    Types that serialize to dicts (BaseModel, dataclass, TypedDict, dict) are stored directly.
    Other types are wrapped in {"items": value} to ensure consistent dict-based storage.
    """

    # Beartype cannot handle the parameterized type annotation (TypeForm[T]) used here for this generic adapter.
    # Using @bear_spray to bypass beartype's runtime checks for this specific method.
    @bear_spray
    def __init__(
        self,
        key_value: AsyncKeyValue,
        pydantic_model: TypeForm[T],
        default_collection: str | None = None,
        raise_on_validation_error: bool = False,
    ) -> None:
        """Create a new PydanticAdapter.

        Args:
            key_value: The KVStore to use.
            pydantic_model: The type to serialize/deserialize. Can be any pydantic-serializable type.
            default_collection: The default collection to use.
            raise_on_validation_error: Whether to raise a DeserializationError if validation fails during reads.
                                       Otherwise, calls will return None if validation fails.
        """
        self._key_value = key_value
        self._type_adapter = TypeAdapter[T](pydantic_model)
        self._default_collection = default_collection
        self._raise_on_validation_error = raise_on_validation_error

        # Determine if this type needs wrapping
        self._needs_wrapping = self._check_needs_wrapping(pydantic_model)

    @bear_spray
    def _check_needs_wrapping(self, type_form: TypeForm[T]) -> bool:
        """Check if a type needs to be wrapped in {"items": ...} for storage.

        Types that serialize to dicts don't need wrapping. Other types do.

        Args:
            type_form: The type to check.

        Returns:
            True if the type needs wrapping, False otherwise.
        """
        # If it serializes to a dict, no wrapping needed
        if self._serializes_to_dict(type_form):
            return False

        # Everything else needs wrapping (lists, primitives, etc.)
        return True

    @bear_spray
    def _serializes_to_dict(self, type_form: TypeForm[T]) -> bool:
        """Check if a type serializes to a dict.

        Args:
            type_form: The type to check.

        Returns:
            True if the type serializes to a dict, False otherwise.
        """
        # Handle generic types (list[...], dict[...], etc.)
        origin = get_origin(type_form)

        # dict and dict[K, V] serialize to dict
        if origin is dict or type_form is dict:
            return True

        # If it's a BaseModel subclass, it serializes to dict
        if isinstance(type_form, type) and issubclass(type_form, BaseModel):
            return True

        # TypedDict is handled by Pydantic and serializes to dict
        # Dataclasses serialize to dict
        # Both are detected via TypeAdapter's core schema, but we can't easily check that here
        # So we use a heuristic: if it's not a known non-dict type, assume it might be dict-serializable

        # Known non-dict types
        non_dict_origins = (list, set, tuple)
        if origin in non_dict_origins:
            return False

        # Primitive types don't serialize to dict
        primitive_types = (int, str, float, bool, bytes, type(None))
        if type_form in primitive_types:
            return False

        # For other types (dataclass, TypedDict, etc.), we assume they serialize to dict
        # This is a heuristic that works for most common cases
        return True

    def _get_model_type_name(self) -> str:
        """Return the model type name for error messages."""
        return "pydantic-serializable value"
