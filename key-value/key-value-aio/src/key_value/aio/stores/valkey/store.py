from typing import overload

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore

try:
    # Use redis-py asyncio client to communicate with a Valkey server (protocol compatible)
    from glide.glide_client import BaseClient, GlideClient
    from glide_shared.commands.core_options import ExpirySet, ExpiryType
    from glide_shared.config import GlideClientConfiguration, NodeAddress, ServerCredentials
except ImportError as e:
    msg = "ValkeyStore requires py-key-value-aio[valkey]"
    raise ImportError(msg) from e


DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000


class ValkeyStore(BaseContextManagerStore, BaseStore):
    """Valkey-based key-value store (Redis protocol compatible)."""

    _connected_client: BaseClient | None
    _client_config: GlideClientConfiguration | None

    @overload
    def __init__(self, *, client: BaseClient, default_collection: str | None = None) -> None: ...

    @overload
    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        default_collection: str | None = None,
    ) -> None: ...

    def __init__(
        self,
        *,
        client: BaseClient | None = None,
        default_collection: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        if client is not None:
            self._connected_client = client
        else:
            # redis client accepts URL
            addresses: list[NodeAddress] = [NodeAddress(host=host, port=port)]
            credentials: ServerCredentials | None = ServerCredentials(password=password, username=username) if password else None
            self._client_config = GlideClientConfiguration(addresses=addresses, database_id=db, credentials=credentials)
            self._connected_client = None

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        if self._connected_client is None:
            if self._client_config is None:
                # This should never happen, makes the type checker happy though
                msg = "Client configuration is not set"
                raise ValueError(msg)

            self._connected_client = await GlideClient.create(config=self._client_config)

    @property
    def _client(self) -> BaseClient:
        if self._connected_client is None:
            # This should never happen, makes the type checker happy though
            msg = "Client is not connected"
            raise ValueError(msg)
        return self._connected_client

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        response: bytes | None = await self._client.get(key=combo_key)
        if not isinstance(response, bytes):
            return None
        decoded_response: str = response.decode("utf-8")
        return ManagedEntry.from_json(json_str=decoded_response)

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        json_value: str = managed_entry.to_json()

        expiry: ExpirySet | None = ExpirySet(expiry_type=ExpiryType.SEC, value=int(managed_entry.ttl)) if managed_entry.ttl else None

        _ = await self._client.set(key=combo_key, value=json_value, expiry=expiry)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)
        return await self._client.delete(keys=[combo_key]) != 0

    @override
    async def _close(self) -> None:
        await self._client.close()
