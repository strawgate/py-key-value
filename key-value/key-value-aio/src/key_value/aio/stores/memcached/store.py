import hashlib
from typing import overload

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyStore, BaseStore

try:
    from aiomcache import Client
except ImportError as e:
    msg = "MemcachedStore requires py-key-value-aio[memcached]"
    raise ImportError(msg) from e

MAX_KEY_LENGTH = 240


class MemcachedStore(BaseDestroyStore, BaseContextManagerStore, BaseStore):
    """Memcached-based key-value store using aiomcache."""

    _client: Client

    @overload
    def __init__(self, *, client: Client, default_collection: str | None = None) -> None: ...

    @overload
    def __init__(self, *, host: str = "127.0.0.1", port: int = 11211, default_collection: str | None = None) -> None: ...

    def __init__(
        self,
        *,
        client: Client | None = None,
        host: str = "127.0.0.1",
        port: int = 11211,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Memcached store.

        Args:
            client: An existing aiomcache client to use.
            host: Memcached host. Defaults to 127.0.0.1.
            port: Memcached port. Defaults to 11211.
            default_collection: The default collection to use if no collection is provided.
        """
        self._client = client or Client(host=host, port=port)

        super().__init__(default_collection=default_collection)

    def sanitize_key(self, key: str) -> str:
        if len(key) > MAX_KEY_LENGTH:
            sha256_hash: str = hashlib.sha256(key.encode()).hexdigest()
            return sha256_hash[:64]
        return key

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = self.sanitize_key(compound_key(collection=collection, key=key))

        raw_value: bytes | None = await self._client.get(combo_key.encode("utf-8"))

        if not isinstance(raw_value, (bytes, bytearray)):
            return None

        json_str: str = raw_value.decode(encoding="utf-8")

        return ManagedEntry.from_json(json_str=json_str)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = self.sanitize_key(compound_key(collection=collection, key=key))

        # Memcached treats 0 as no-expiration. Do not pass <= 0 (other than 0) to avoid permanence errors.
        exptime: int

        if managed_entry.ttl is None:  # noqa: SIM108
            exptime = 0
        else:
            exptime = max(int(managed_entry.ttl), 1)

        json_value: str = managed_entry.to_json()

        _ = await self._client.set(
            key=combo_key.encode(encoding="utf-8"),
            value=json_value.encode(encoding="utf-8"),
            exptime=exptime,
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = self.sanitize_key(compound_key(collection=collection, key=key))

        return await self._client.delete(key=combo_key.encode(encoding="utf-8"))

    @override
    async def _delete_store(self) -> bool:
        _ = await self._client.flush_all()
        return True

    @override
    async def _close(self) -> None:
        await self._client.close()
