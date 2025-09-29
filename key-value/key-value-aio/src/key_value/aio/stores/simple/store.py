from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from key_value.shared.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from key_value.shared.utils.managed_entry import ManagedEntry, load_from_json
from key_value.shared.utils.time_to_live import seconds_to
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseDestroyStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
    BaseStore,
)

DEFAULT_SIMPLE_STORE_MAX_ENTRIES = 10000


@dataclass
class SimpleStoreEntry:
    json_str: str

    created_at: datetime | None
    expires_at: datetime | None

    @property
    def current_ttl(self) -> float | None:
        if self.expires_at is None:
            return None

        return seconds_to(datetime=self.expires_at)

    def to_managed_entry(self) -> ManagedEntry:
        managed_entry: ManagedEntry = ManagedEntry(
            value=load_from_json(json_str=self.json_str),
            expires_at=self.expires_at,
            created_at=self.created_at,
        )

        return managed_entry


class SimpleStore(BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseDestroyStore, BaseStore):
    """Simple managed dictionary-based key-value store for testing and development."""

    max_entries: int

    _data: dict[str, SimpleStoreEntry]

    def __init__(self, max_entries: int = DEFAULT_SIMPLE_STORE_MAX_ENTRIES, default_collection: str | None = None):
        """Initialize the simple store.

        Args:
            max_entries: The maximum number of entries to store. Defaults to 10000.
            default_collection: The default collection to use if no collection is provided.
        """

        self.max_entries = max_entries

        self._data = defaultdict[str, SimpleStoreEntry]()

        super().__init__(default_collection=default_collection)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        store_entry: SimpleStoreEntry | None = self._data.get(combo_key)

        if store_entry is None:
            return None

        return store_entry.to_managed_entry()

    @override
    async def _put_managed_entry(self, *, key: str, collection: str, managed_entry: ManagedEntry) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        if len(self._data) >= self.max_entries:
            _ = self._data.pop(next(iter(self._data)))

        self._data[combo_key] = SimpleStoreEntry(
            json_str=managed_entry.to_json(include_metadata=False), expires_at=managed_entry.expires_at, created_at=managed_entry.created_at
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return self._data.pop(combo_key, None) is not None

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        return get_keys_from_compound_keys(compound_keys=list(self._data.keys()), collection=collection)

    @override
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        return get_collections_from_compound_keys(compound_keys=list(self._data.keys()))

    @override
    async def _delete_store(self) -> bool:
        self._data.clear()
        return True
