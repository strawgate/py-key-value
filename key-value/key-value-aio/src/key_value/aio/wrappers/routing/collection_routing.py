from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.routing.wrapper import RoutingWrapper


class CollectionRoutingWrapper(RoutingWrapper):
    """Routes operations based on collection name using a simple map.

    This is a convenience wrapper that provides collection-based routing using a
    dictionary mapping collection names to stores. This is useful for directing
    different data types to different backing stores.

    Example:
        router = CollectionRoutingWrapper(
            collection_map={
                "sessions": redis_store,
                "users": dynamo_store,
                "cache": memory_store,
            },
            default_store=disk_store
        )

        # Gets from redis_store
        await router.get("session_id", collection="sessions")

        # Gets from dynamo_store
        await router.get("user_id", collection="users")

        # Gets from disk_store (default)
        await router.get("other_key", collection="unmapped_collection")
    """

    def __init__(
        self,
        collection_map: dict[str, AsyncKeyValue],
        default_store: AsyncKeyValue | None = None,
    ) -> None:
        """Initialize collection-based routing.

        Args:
            collection_map: Mapping from collection name to store. Each collection
                           name is mapped to its corresponding backing store.
            default_store: Store to use for unmapped collections. If None and a
                          collection is not in the map, operations will raise ValueError.
        """
        self.collection_map = collection_map

        def route_by_collection(collection: str | None) -> AsyncKeyValue | None:
            if collection is None:
                return default_store
            return self.collection_map.get(collection, default_store)

        super().__init__(
            routing_function=route_by_collection,
            default_store=default_store,
        )
