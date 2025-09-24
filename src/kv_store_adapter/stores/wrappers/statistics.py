from dataclasses import dataclass, field
from typing import Any

from typing_extensions import override

from kv_store_adapter.stores.base.unmanaged import BaseKVStore
from kv_store_adapter.types import TTLInfo


@dataclass
class BaseStatistics:
    """Base statistics container with operation counting."""

    count: int = field(default=0)
    """The number of operations."""

    def increment(self) -> None:
        self.count += 1


@dataclass
class BaseHitMissStatistics(BaseStatistics):
    """Statistics container with hit/miss tracking for cache-like operations."""

    hit: int = field(default=0)
    """The number of hits."""
    miss: int = field(default=0)
    """The number of misses."""

    def increment_hit(self) -> None:
        self.increment()
        self.hit += 1

    def increment_miss(self) -> None:
        self.increment()
        self.miss += 1


@dataclass
class GetStatistics(BaseHitMissStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class SetStatistics(BaseStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class DeleteStatistics(BaseHitMissStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class ExistsStatistics(BaseHitMissStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class KeysStatistics(BaseStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class ClearCollectionStatistics(BaseHitMissStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class ListCollectionsStatistics(BaseStatistics):
    """A class for statistics about a KV Store collection."""


@dataclass
class KVStoreCollectionStatistics(BaseStatistics):
    """A class for statistics about a KV Store collection."""

    get: GetStatistics = field(default_factory=GetStatistics)
    """The statistics for the get operation."""

    set: SetStatistics = field(default_factory=SetStatistics)
    """The statistics for the set operation."""

    delete: DeleteStatistics = field(default_factory=DeleteStatistics)
    """The statistics for the delete operation."""

    exists: ExistsStatistics = field(default_factory=ExistsStatistics)
    """The statistics for the exists operation."""

    keys: KeysStatistics = field(default_factory=KeysStatistics)
    """The statistics for the keys operation."""

    clear_collection: ClearCollectionStatistics = field(default_factory=ClearCollectionStatistics)
    """The statistics for the clear collection operation."""

    list_collections: ListCollectionsStatistics = field(default_factory=ListCollectionsStatistics)
    """The statistics for the list collections operation."""


@dataclass
class KVStoreStatistics:
    """Statistics container for a KV Store."""

    collections: dict[str, KVStoreCollectionStatistics] = field(default_factory=dict)

    def get_collection(self, collection: str) -> KVStoreCollectionStatistics:
        if collection not in self.collections:
            self.collections[collection] = KVStoreCollectionStatistics()
        return self.collections[collection]


class StatisticsWrapper(BaseKVStore):
    """Statistics wrapper around a KV Store that tracks operation statistics."""

    def __init__(self, store: BaseKVStore, track_statistics: bool = True) -> None:
        self.store: BaseKVStore = store
        self._statistics: KVStoreStatistics | None = KVStoreStatistics() if track_statistics else None

    @property
    def statistics(self) -> KVStoreStatistics | None:
        return self._statistics

    @override
    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        if value := await self.store.get(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).get.increment_hit()
            return value

        if self.statistics:
            self.statistics.get_collection(collection).get.increment_miss()

        return None

    @override
    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        await self.store.put(collection=collection, key=key, value=value, ttl=ttl)

        if self.statistics:
            self.statistics.get_collection(collection).set.increment()

    @override
    async def delete(self, collection: str, key: str) -> bool:
        if await self.store.delete(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).delete.increment_hit()
            return True

        if self.statistics:
            self.statistics.get_collection(collection).delete.increment_miss()

        return False

    @override
    async def exists(self, collection: str, key: str) -> bool:
        if await self.store.exists(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).exists.increment_hit()
            return True

        if self.statistics:
            self.statistics.get_collection(collection).exists.increment_miss()

        return False

    @override
    async def keys(self, collection: str) -> list[str]:
        keys: list[str] = await self.store.keys(collection)

        if self.statistics:
            self.statistics.get_collection(collection).keys.increment()

        return keys

    @override
    async def clear_collection(self, collection: str) -> int:
        if count := await self.store.clear_collection(collection):
            if self.statistics:
                self.statistics.get_collection(collection).clear_collection.increment_hit()
            return count

        if self.statistics:
            self.statistics.get_collection(collection).clear_collection.increment_miss()

        return 0

    @override
    async def ttl(self, collection: str, key: str) -> TTLInfo | None:
        return await self.store.ttl(collection=collection, key=key)

    @override
    async def list_collections(self) -> list[str]:
        return await self.store.list_collections()

    @override
    async def cull(self) -> None:
        await self.store.cull()
