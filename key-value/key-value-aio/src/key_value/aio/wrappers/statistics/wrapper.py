from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, SupportsFloat

from typing_extensions import override

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.base import DEFAULT_COLLECTION_NAME
from key_value.aio.wrappers.base import BaseWrapper


@dataclass
class BaseStatistics:
    """Base statistics container with operation counting."""

    count: int = field(default=0)
    """The number of operations."""

    def increment(self, *, increment: int = 1) -> None:
        self.count += increment


@dataclass
class BaseHitMissStatistics(BaseStatistics):
    """Statistics container with hit/miss tracking for cache-like operations."""

    hit: int = field(default=0)
    """The number of hits."""
    miss: int = field(default=0)
    """The number of misses."""

    def increment_hit(self, *, increment: int = 1) -> None:
        self.increment(increment=increment)
        self.hit += increment

    def increment_miss(self, *, increment: int = 1) -> None:
        self.increment(increment=increment)
        self.miss += increment


@dataclass
class GetStatistics(BaseHitMissStatistics):
    """Statistics for get operations.

    Tracks the number of successful retrievals (hits) vs. cache misses for get operations.
    """


@dataclass
class PutStatistics(BaseStatistics):
    """Statistics for put operations.

    Tracks the total number of put (write) operations performed.
    """


@dataclass
class DeleteStatistics(BaseHitMissStatistics):
    """Statistics for delete operations.

    Tracks the number of successful deletions (hits) vs. attempted deletions of non-existent keys (misses).
    """


@dataclass
class ExistsStatistics(BaseHitMissStatistics):
    """Statistics for exists operations.

    Tracks the number of keys that exist (hits) vs. keys that don't exist (misses) when checking for existence.
    """


@dataclass
class TTLStatistics(BaseHitMissStatistics):
    """Statistics for TTL operations.

    Tracks the number of successful TTL retrievals (hits) vs. TTL queries for non-existent keys (misses).
    """


@dataclass
class KVStoreCollectionStatistics(BaseStatistics):
    """Aggregated statistics for all operations on a single collection.

    This dataclass groups together statistics for all operation types (get, put, delete,
    ttl, exists) performed on a single collection, providing a comprehensive view of
    collection-level activity.
    """

    get: GetStatistics = field(default_factory=GetStatistics)
    """The statistics for the get operation."""

    ttl: TTLStatistics = field(default_factory=TTLStatistics)
    """The statistics for the ttl operation."""

    put: PutStatistics = field(default_factory=PutStatistics)
    """The statistics for the put operation."""

    delete: DeleteStatistics = field(default_factory=DeleteStatistics)
    """The statistics for the delete operation."""

    exists: ExistsStatistics = field(default_factory=ExistsStatistics)
    """The statistics for the exists operation."""


@dataclass
class KVStoreStatistics:
    """Statistics container for an entire KV Store across all collections.

    This class maintains a dictionary of per-collection statistics, allowing tracking
    and analysis of operations across all collections in the store.
    """

    collections: dict[str, KVStoreCollectionStatistics] = field(default_factory=dict)

    def get_collection(self, collection: str) -> KVStoreCollectionStatistics:
        """Get or create statistics for a specific collection.

        Args:
            collection: The collection name.

        Returns:
            The statistics object for the specified collection, creating it if it doesn't exist.
        """
        if collection not in self.collections:
            self.collections[collection] = KVStoreCollectionStatistics()
        return self.collections[collection]


class StatisticsWrapper(BaseWrapper):
    """Statistics wrapper around a KV Store that tracks operation statistics.

    Note: enumeration and destroy operations are not tracked by this wrapper.
    """

    def __init__(self, key_value: AsyncKeyValue) -> None:
        self.key_value: AsyncKeyValue = key_value
        self._statistics: KVStoreStatistics = KVStoreStatistics()

    @property
    def statistics(self) -> KVStoreStatistics:
        return self._statistics

    @override
    async def get(self, key: str, *, collection: str | None = None) -> dict[str, Any] | None:
        collection = collection or DEFAULT_COLLECTION_NAME

        if value := await self.key_value.get(collection=collection, key=key):
            self.statistics.get_collection(collection=collection).get.increment_hit()
            return value

        self.statistics.get_collection(collection=collection).get.increment_miss()

        return None

    @override
    async def ttl(self, key: str, *, collection: str | None = None) -> tuple[dict[str, Any] | None, float | None]:
        collection = collection or DEFAULT_COLLECTION_NAME

        value, ttl = await self.key_value.ttl(collection=collection, key=key)

        if value:
            self.statistics.get_collection(collection=collection).ttl.increment_hit()
            return value, ttl

        self.statistics.get_collection(collection=collection).ttl.increment_miss()
        return None, None

    @override
    async def put(self, key: str, value: Mapping[str, Any], *, collection: str | None = None, ttl: SupportsFloat | None = None) -> None:
        collection = collection or DEFAULT_COLLECTION_NAME

        await self.key_value.put(collection=collection, key=key, value=value, ttl=ttl)

        self.statistics.get_collection(collection=collection).put.increment()

    @override
    async def delete(self, key: str, *, collection: str | None = None) -> bool:
        collection = collection or DEFAULT_COLLECTION_NAME

        if await self.key_value.delete(collection=collection, key=key):
            self.statistics.get_collection(collection=collection).delete.increment_hit()
            return True

        self.statistics.get_collection(collection=collection).delete.increment_miss()

        return False

    @override
    async def get_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[dict[str, Any] | None]:
        collection = collection or DEFAULT_COLLECTION_NAME

        results: list[dict[str, Any] | None] = await self.key_value.get_many(keys=keys, collection=collection)

        hits = len([result for result in results if result is not None])
        misses = len([result for result in results if result is None])

        self.statistics.get_collection(collection=collection).get.increment_hit(increment=hits)
        self.statistics.get_collection(collection=collection).get.increment_miss(increment=misses)

        return results

    @override
    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        collection = collection or DEFAULT_COLLECTION_NAME

        await self.key_value.put_many(keys=keys, values=values, collection=collection, ttl=ttl)

        self.statistics.get_collection(collection=collection).put.increment(increment=len(keys))

    @override
    async def delete_many(self, keys: Sequence[str], *, collection: str | None = None) -> int:
        collection = collection or DEFAULT_COLLECTION_NAME

        deleted_count: int = await self.key_value.delete_many(keys=keys, collection=collection)

        hits = deleted_count
        misses = len(keys) - deleted_count

        self.statistics.get_collection(collection=collection).delete.increment_hit(increment=hits)
        self.statistics.get_collection(collection=collection).delete.increment_miss(increment=misses)

        return deleted_count

    @override
    async def ttl_many(self, keys: Sequence[str], *, collection: str | None = None) -> list[tuple[dict[str, Any] | None, float | None]]:
        collection = collection or DEFAULT_COLLECTION_NAME

        results: list[tuple[dict[str, Any] | None, float | None]] = await self.key_value.ttl_many(keys=keys, collection=collection)

        hits = len([result for result in results if result[0] is not None])
        misses = len([result for result in results if result[0] is None])

        self.statistics.get_collection(collection=collection).ttl.increment_hit(increment=hits)
        self.statistics.get_collection(collection=collection).ttl.increment_miss(increment=misses)

        return results
