from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry
from kv_store_adapter.stores.utils.time_to_live import calculate_expires_at
from kv_store_adapter.types import TTLInfo

DEFAULT_SIMPLE_MANAGED_STORE_MAX_ENTRIES = 1000
DEFAULT_SIMPLE_STORE_MAX_ENTRIES = 1000


class SimpleStore(BaseKVStore):
    """Simple dictionary-based key-value store for testing and development."""

    max_entries: int
    _data: dict[str, dict[str, Any]]
    _expirations: dict[str, datetime]

    def __init__(self, max_entries: int = DEFAULT_SIMPLE_STORE_MAX_ENTRIES):
        super().__init__()
        self.max_entries = max_entries
        self._data = defaultdict[str, dict[str, Any]](dict)
        self._expirations = defaultdict[str, datetime]()

    async def setup(self) -> None:
        pass

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        combo_key: str = compound_key(collection=collection, key=key)

        if not (data := self._data.get(combo_key)):
            return None

        if not (expiration := self._expirations.get(combo_key)):
            return data

        if expiration <= datetime.now(tz=timezone.utc):
            del self._data[combo_key]
            del self._expirations[combo_key]
            return None

        return data

    @override
    async def exists(self, collection: str, key: str) -> bool:
        return await self.get(collection=collection, key=key) is not None

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        if len(self._data) >= self.max_entries:
            _ = self._data.pop(next(iter(self._data)))

        _ = self._data[combo_key] = value

        if expires_at := calculate_expires_at(ttl=ttl):
            _ = self._expirations[combo_key] = expires_at

    @override
    async def delete(self, collection: str, key: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        return self._data.pop(combo_key, None) is not None

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        combo_key: str = compound_key(collection=collection, key=key)

        if not (expiration := self._expirations.get(combo_key)):
            return None

        return TTLInfo(collection=collection, key=key, created_at=None, ttl=None, expires_at=expiration)

    @override
    async def keys(self, collection: str) -> list[str]:
        return get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        keys: list[str] = get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection)

        for key in keys:
            _ = self._data.pop(key)
            _ = self._expirations.pop(key)

        return len(keys)

    @override
    async def list_collections(self) -> list[str]:
        return get_collections_from_compound_keys(compound_keys=list(self._data.keys()))

    @override
    async def cull(self) -> None:
        for collection in await self.list_collections():
            for key in get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection):
                if not (expiration := self._expirations.get(key)):
                    continue

                if expiration <= datetime.now(tz=timezone.utc):
                    _ = self._data.pop(key)
                    _ = self._expirations.pop(key)


class SimpleManagedStore(BaseManagedKVStore):
    """Simple managed dictionary-based key-value store for testing and development."""

    max_entries: int
    _data: dict[str, ManagedEntry]

    def __init__(self, max_entries: int = DEFAULT_SIMPLE_MANAGED_STORE_MAX_ENTRIES):
        super().__init__()
        self.max_entries = max_entries
        self._data = defaultdict[str, ManagedEntry]()

    @override
    async def setup(self) -> None:
        pass

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)
        return self._data.get(combo_key)

    @override
    async def put_entry(self, collection: str, key: str, cache_entry: ManagedEntry, *, ttl: float | None = None) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        if len(self._data) >= self.max_entries:
            _ = self._data.pop(next(iter(self._data)))

        self._data[combo_key] = cache_entry

    @override
    async def delete(self, collection: str, key: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        return self._data.pop(combo_key, None) is not None

    @override
    async def keys(self, collection: str) -> list[str]:
        return get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        keys: list[str] = get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection)

        for key in keys:
            _ = self._data.pop(key)

        return len(keys)

    @override
    async def list_collections(self) -> list[str]:
        return get_collections_from_compound_keys(compound_keys=list(self._data.keys()))

    @override
    async def cull(self) -> None:
        for collection in await self.list_collections():
            for key in get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection):
                _ = await self.get_entry(collection=collection, key=key)
