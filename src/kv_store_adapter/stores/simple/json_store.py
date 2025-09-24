from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

DEFAULT_SIMPLE_JSON_STORE_MAX_ENTRIES = 1000


class SimpleJSONStore(BaseManagedKVStore):
    """Simple JSON-serialized dictionary-based key-value store for testing."""

    max_entries: int
    _data: dict[str, str]

    def __init__(self, max_entries: int = DEFAULT_SIMPLE_JSON_STORE_MAX_ENTRIES):
        super().__init__()
        self.max_entries = max_entries
        self._data = {}

    @override
    async def setup(self) -> None:
        pass

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        if not (data := self._data.get(combo_key)):
            return None

        return ManagedEntry.from_json(json_str=data)

    @override
    async def put_entry(self, collection: str, key: str, cache_entry: ManagedEntry, *, ttl: float | None = None) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        if len(self._data) >= self.max_entries:
            _ = self._data.pop(next(iter(self._data)))

        self._data[combo_key] = cache_entry.to_json()

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
