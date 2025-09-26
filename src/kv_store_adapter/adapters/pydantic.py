from collections.abc import Sequence
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError

from kv_store_adapter.errors import DeserializationError, SerializationError
from kv_store_adapter.types import KVStore

T = TypeVar("T", bound=BaseModel)


class PydanticAdapter(Generic[T]):
    """Adapter around a KVStore-compliant Store that allows type-safe persistence of Pydantic models."""

    def __init__(self, kv_store: KVStore, pydantic_model: type[T], default_collection: str | None = None) -> None:
        self.kv_store: KVStore = kv_store
        self.pydantic_model: type[T] = pydantic_model
        self.default_collection: str | None = default_collection

    def _validate_model(self, value: dict[str, Any]) -> T:
        try:
            return self.pydantic_model.model_validate(obj=value)
        except ValidationError as e:
            msg = f"Invalid Pydantic model: {e}"
            raise DeserializationError(msg) from e

    def _serialize_model(self, value: T) -> dict[str, Any]:
        try:
            return value.model_dump(mode="json")
        except PydanticSerializationError as e:
            msg = f"Invalid Pydantic model: {e}"
            raise SerializationError(msg) from e

    async def get(self, key: str, *, collection: str | None = None) -> T | None:
        """Get and validate a model by key.

        Returns the parsed model instance, or None if not present.
        Raises DeserializationError if the stored data cannot be validated as the model.
        """
        collection = collection or self.default_collection

        if value := await self.kv_store.get(key=key, collection=collection):
            return self._validate_model(value=value)

        return None

    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[T | None]:
        """Batch get and validate models by keys, preserving order.

        Each element is either a parsed model instance or None if missing.
        """
        collection = collection or self.default_collection

        values: list[dict[str, Any] | None] = await self.kv_store.get_many(keys=keys, collection=collection)

        return [self._validate_model(value=value) if value else None for value in values]

    async def put(self, key: str, value: T, *, collection: str | None = None, ttl: float | None = None) -> None:
        """Serialize and store a model.

        Propagates SerializationError if the model cannot be serialized.
        """
        collection = collection or self.default_collection

        value_dict: dict[str, Any] = self._serialize_model(value=value)

        await self.kv_store.put(key=key, value=value_dict, collection=collection, ttl=ttl)

    async def put_many(self, keys: Sequence[str], values: Sequence[T], *, collection: str | None = None, ttl: float | None = None) -> None:
        """Serialize and store multiple models, preserving order alignment with keys."""
        collection = collection or self.default_collection

        value_dicts: list[dict[str, Any]] = [self._serialize_model(value=value) for value in values]

        await self.kv_store.put_many(keys=keys, values=value_dicts, collection=collection, ttl=ttl)

    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        """Delete a model by key. Returns True if a value was deleted, else False."""
        collection = collection or self.default_collection

        return await self.kv_store.delete(key=key, collection=collection)

    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        """Delete multiple models by key. Returns the count of deleted entries."""
        collection = collection or self.default_collection

        return await self.kv_store.delete_many(keys=keys, collection=collection)

    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[T | None, float | None]:
        """Get a model and its TTL seconds if present.

        Returns (model, ttl_seconds) or (None, None) if missing.
        """
        collection = collection or self.default_collection

        entry: dict[str, Any] | None
        ttl_info: float | None

        entry, ttl_info = await self.kv_store.ttl(key=key, collection=collection)

        if entry is not None:
            model_validate: T = self._validate_model(value=entry)
            return (model_validate, ttl_info)

        return (None, None)

    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[T | None, float | None]]:
        """Batch get models with TTLs. Each element is (model|None, ttl_seconds|None)."""
        collection = collection or self.default_collection

        entries: list[tuple[dict[str, Any] | None, float | None]] = await self.kv_store.ttl_many(keys=keys, collection=collection)

        return [(self._validate_model(value=entry) if entry else None, ttl_info) for entry, ttl_info in entries]
