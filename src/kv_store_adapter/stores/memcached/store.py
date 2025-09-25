import hashlib
from typing import Any, overload

from pymemcache.client.base import Client
from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

# Memcached key length limit
MEMCACHED_MAX_KEY_LENGTH = 250


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

    def _get_safe_key(self, combo_key: str) -> str:
        """Get a safe key for memcached, hashing if necessary."""
        if len(combo_key) > MEMCACHED_MAX_KEY_LENGTH:
            # Use MD5 hash for long keys - this is not for security, just for key shortening
            return hashlib.md5(combo_key.encode()).hexdigest()  # noqa: S324
        return combo_key

    def _test_connection(self) -> None:
        """Test the memcached connection."""
        test_key = "__memcached_test__"
        self._client.set(test_key, "test_value", expire=1)
        result = self._client.get(test_key)
        if result is None:
            msg = "Failed to connect to Memcached"
            raise StoreConnectionError(message=msg)
        # Clean up test key
        self._client.delete(test_key)

    @override
    async def setup(self) -> None:
        # Test the connection by performing a simple operation
        try:
            self._test_connection()
        except Exception as e:
            msg = f"Failed to connect to Memcached: {e}"
            raise StoreConnectionError(message=msg) from e

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)
        safe_key = self._get_safe_key(combo_key)

        cache_entry: Any = self._client.get(safe_key)

        if cache_entry is None:
            return None

        if not isinstance(cache_entry, (str, bytes)):
            return None

        # Convert bytes to string if necessary
        if isinstance(cache_entry, bytes):
            cache_entry = cache_entry.decode("utf-8")

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
        safe_key = self._get_safe_key(combo_key)

        json_value: str = cache_entry.to_json()

        if ttl is not None:
            # Memcached TTL must be an integer
            ttl = max(int(ttl), 1)
            self._client.set(safe_key, json_value, expire=ttl)
        else:
            self._client.set(safe_key, json_value)

    @override
    async def delete(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        combo_key: str = compound_key(collection=collection, key=key)
        safe_key = self._get_safe_key(combo_key)

        return self._client.delete(safe_key)

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
