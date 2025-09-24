from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError

from kv_store_adapter.errors import DeserializationError, SerializationError
from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo

T = TypeVar("T", bound=BaseModel)


class PydanticAdapter(Generic[T]):
    """Adapter around a KV Store that allows type-safe persistence of Pydantic models."""

    def __init__(self, store: BaseKVStore, pydantic_model: type[T]) -> None:
        self.store: BaseKVStore = store
        self.pydantic_model: type[T] = pydantic_model

    async def get(self, collection: str, key: str) -> T | None:
        if value := await self.store.get(collection=collection, key=key):
            try:
                return self.pydantic_model.model_validate(obj=value)
            except ValidationError as e:
                msg = f"Invalid Pydantic model: {e}"
                raise DeserializationError(msg) from e

        return None

    async def put(self, collection: str, key: str, value: T, *, ttl: float | None = None) -> None:
        try:
            value_dict: dict[str, Any] = value.model_dump()
        except PydanticSerializationError as e:
            msg = f"Invalid Pydantic model: {e}"
            raise SerializationError(msg) from e

        await self.store.put(collection=collection, key=key, value=value_dict, ttl=ttl)

    async def delete(self, collection: str, key: str) -> bool:
        return await self.store.delete(collection=collection, key=key)

    async def exists(self, collection: str, key: str) -> bool:
        return await self.store.exists(collection=collection, key=key)

    async def keys(self, collection: str) -> list[str]:
        return await self.store.keys(collection=collection)

    async def clear_collection(self, collection: str) -> int:
        return await self.store.clear_collection(collection=collection)

    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        return await self.store.ttl(collection=collection, key=key)

    async def list_collections(self) -> list[str]:
        return await self.store.list_collections()

    async def cull(self) -> None:
        await self.store.cull()
