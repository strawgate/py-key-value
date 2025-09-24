from typing import Any, overload
from urllib.parse import urlparse

from redis.asyncio import Redis
from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_keys_from_compound_keys, uncompound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


class RedisStore(BaseManagedKVStore):
    """Redis-based key-value store."""

    _client: Redis

    @overload
    def __init__(self, *, client: Redis) -> None: ...

    @overload
    def __init__(self, *, url: str) -> None: ...

    @overload
    def __init__(self, *, host: str = "localhost", port: int = 6379, db: int = 0, password: str | None = None) -> None: ...

    def __init__(
        self,
        *,
        client: Redis | None = None,
        url: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
    ) -> None:
        """Initialize the Redis store.

        Args:
            client: An existing Redis client to use.
            url: Redis URL (e.g., redis://localhost:6379/0).
            host: Redis host. Defaults to localhost.
            port: Redis port. Defaults to 6379.
            db: Redis database number. Defaults to 0.
            password: Redis password. Defaults to None.
        """
        if client:
            self._client = client
        elif url:
            parsed_url = urlparse(url)
            self._client = Redis(
                host=parsed_url.hostname or "localhost",
                port=parsed_url.port or 6379,
                db=int(parsed_url.path.lstrip("/")) if parsed_url.path and parsed_url.path != "/" else 0,
                password=parsed_url.password or password,
                decode_responses=True,
            )
        else:
            self._client = Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
            )

        super().__init__()

    @override
    async def setup(self) -> None:
        if not await self._client.ping():  # pyright: ignore[reportUnknownMemberType]
            raise StoreConnectionError(message="Failed to connect to Redis")

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        cache_entry: Any = await self._client.get(name=combo_key)  # pyright: ignore[reportAny]

        if cache_entry is None:
            return None

        if not isinstance(cache_entry, str):
            return None

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

        json_value: str = cache_entry.to_json()

        if ttl is not None:
            # Redis does not support <= 0 TTLs
            ttl = max(int(ttl), 1)

            _ = await self._client.setex(name=combo_key, time=ttl, value=json_value)  # pyright: ignore[reportAny]
        else:
            _ = await self._client.set(name=combo_key, value=json_value)  # pyright: ignore[reportAny]

    @override
    async def delete(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        combo_key: str = compound_key(collection=collection, key=key)
        return await self._client.delete(combo_key) != 0  # pyright: ignore[reportAny]

    @override
    async def keys(self, collection: str) -> list[str]:
        await self.setup_collection_once(collection=collection)

        pattern = compound_key(collection=collection, key="*")
        compound_keys: list[str] = await self._client.keys(pattern)  # pyright: ignore[reportUnknownMemberType, reportAny]

        return get_keys_from_compound_keys(compound_keys=compound_keys, collection=collection)

    @override
    async def clear_collection(self, collection: str) -> int:
        await self.setup_collection_once(collection=collection)

        pattern = compound_key(collection=collection, key="*")

        deleted_count: int = 0

        async for key in self._client.scan_iter(name=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not isinstance(key, str):
                continue

            deleted_count += await self._client.delete(key)  # pyright: ignore[reportAny]

        return deleted_count

    @override
    async def list_collections(self) -> list[str]:
        await self.setup_once()

        pattern: str = compound_key(collection="*", key="*")

        collections: set[str] = set()

        async for key in self._client.scan_iter(name=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if not isinstance(key, str):
                continue

            collections.add(uncompound_key(key=key)[0])

        return list[str](collections)

    @override
    async def cull(self) -> None: ...
