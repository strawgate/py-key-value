from typing import Any, overload

from pymemcache.client.base import Client
from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_keys_from_compound_keys, uncompound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


class MemcachedStore(BaseManagedKVStore):
    """Memcached-based key-value store."""

    _client: Client

    @overload
    def __init__(self, *, client: Client) -> None: ...

    @overload
    def __init__(self, *, host: str = "localhost", port: int = 11211) -> None: ...

    def __init__(
        self,
        *,
        client: Client | None = None,
        host: str = "localhost",
        port: int = 11211,
    ) -> None:
        """Initialize the Memcached store.

        Args:
            client: An existing pymemcache Client to use.
            host: Memcached host. Defaults to localhost.
            port: Memcached port. Defaults to 11211.
        """
        if client:
            self._client = client
        else:
            self._client = Client((host, port))

        super().__init__()

    @override
    async def setup(self) -> None:
        # Test the connection by performing a simple operation
        try:
            # Try to set and get a test key to verify connection
            test_key = "__memcached_test__"
            self._client.set(test_key, "test_value", expire=1)
            result = self._client.get(test_key)
            if result is None:
                raise StoreConnectionError(message="Failed to connect to Memcached")
            # Clean up test key
            self._client.delete(test_key)
        except Exception as e:
            raise StoreConnectionError(message=f"Failed to connect to Memcached: {e}") from e

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        # Memcached keys must be strings and under 250 characters
        # Use a hash if the key is too long
        if len(combo_key) > 250:
            import hashlib
            combo_key = hashlib.md5(combo_key.encode()).hexdigest()

        cache_entry: Any = self._client.get(combo_key)

        if cache_entry is None:
            return None

        if not isinstance(cache_entry, (str, bytes)):
            return None

        # Convert bytes to string if necessary
        if isinstance(cache_entry, bytes):
            cache_entry = cache_entry.decode('utf-8')

        return ManagedEntry.from_json(json_str=cache_entry)

    @override
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        # Memcached keys must be strings and under 250 characters
        # Use a hash if the key is too long
        if len(combo_key) > 250:
            import hashlib
            combo_key = hashlib.md5(combo_key.encode()).hexdigest()

        json_value: str = cache_entry.to_json()

        if ttl is not None:
            # Memcached TTL must be an integer
            ttl = max(int(ttl), 1)
            self._client.set(combo_key, json_value, expire=ttl)
        else:
            self._client.set(combo_key, json_value)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        combo_key: str = compound_key(collection=collection, key=key)

        # Memcached keys must be strings and under 250 characters
        # Use a hash if the key is too long
        if len(combo_key) > 250:
            import hashlib
            combo_key = hashlib.md5(combo_key.encode()).hexdigest()

        return self._client.delete(combo_key)

    @override
    async def keys(self, collection: str) -> list[str]:
        await self.setup_collection_once(collection=collection)

        # Memcached doesn't support pattern matching or listing keys
        # This is a limitation of memcached - we return an empty list
        # In practice, applications should track keys separately if needed
        return []

    @override
    async def clear_collection(self, collection: str) -> int:
        await self.setup_collection_once(collection=collection)

        # Memcached doesn't support pattern matching or selective deletion
        # This is a limitation of memcached - we return 0
        # In practice, applications would need to track keys separately
        return 0

    @override
    async def list_collections(self) -> list[str]:
        await self.setup_once()

        # Memcached doesn't support listing all keys or pattern matching
        # This is a limitation of memcached - we return an empty list
        return []

    @override
    async def cull(self) -> None:
        # Memcached handles expiration automatically
        # No need to manually cull expired entries
        pass