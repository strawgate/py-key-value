from dataclasses import dataclass, field
from typing import Any

from kv_store_adapter.types import KVStoreProtocol


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
    """A class for statistics about GET operations."""


@dataclass
class SetStatistics(BaseStatistics):
    """A class for statistics about PUT operations."""


@dataclass
class DeleteStatistics(BaseHitMissStatistics):
    """A class for statistics about DELETE operations."""


@dataclass
class ExistsStatistics(BaseHitMissStatistics):
    """A class for statistics about EXISTS operations."""


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


@dataclass
class KVStoreStatistics:
    """Statistics container for a KV Store."""

    collections: dict[str, KVStoreCollectionStatistics] = field(default_factory=dict)

    def get_collection(self, collection: str) -> KVStoreCollectionStatistics:
        if collection not in self.collections:
            self.collections[collection] = KVStoreCollectionStatistics()
        return self.collections[collection]


class StatisticsWrapper:
    """Statistics wrapper around a KV Store that tracks operation statistics."""

    def __init__(self, store: KVStoreProtocol, track_statistics: bool = True) -> None:
        self.store: KVStoreProtocol = store
        self._statistics: KVStoreStatistics | None = KVStoreStatistics() if track_statistics else None

    @property
    def statistics(self) -> KVStoreStatistics | None:
        return self._statistics

    async def get(self, collection: str, key: str) -> dict[str, Any] | None:
        if value := await self.store.get(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).get.increment_hit()
            return value

        if self.statistics:
            self.statistics.get_collection(collection).get.increment_miss()

        return None

    async def put(self, collection: str, key: str, value: dict[str, Any], *, ttl: float | None = None) -> None:
        await self.store.put(collection=collection, key=key, value=value, ttl=ttl)

        if self.statistics:
            self.statistics.get_collection(collection).set.increment()

    async def delete(self, collection: str, key: str) -> bool:
        if await self.store.delete(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).delete.increment_hit()
            return True

        if self.statistics:
            self.statistics.get_collection(collection).delete.increment_miss()

        return False

    async def exists(self, collection: str, key: str) -> bool:
        if await self.store.exists(collection=collection, key=key):
            if self.statistics:
                self.statistics.get_collection(collection).exists.increment_hit()
            return True

        if self.statistics:
            self.statistics.get_collection(collection).exists.increment_miss()

        return False