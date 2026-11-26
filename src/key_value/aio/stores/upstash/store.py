from collections.abc import Sequence
from datetime import datetime
from typing import overload

from typing_extensions import override

from key_value.aio._utils.beartype import bear_spray
from key_value.aio._utils.compound import compound_key, get_keys_from_compound_keys
from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio._utils.serialization import BasicSerializationAdapter, SerializationAdapter
from key_value.aio.errors import DeserializationError
from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyStore, BaseEnumerateKeysStore, BaseStore

try:
    from upstash_redis.asyncio import Redis
except ImportError as e:
    msg = "UpstashRedisStore requires py-key-value-aio[upstash]"
    raise ImportError(msg) from e

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000


class UpstashRedisStore(BaseDestroyStore, BaseEnumerateKeysStore, BaseContextManagerStore, BaseStore):
    """[Upstash Redis](https://upstash.com/docs/redis/overall/getstarted) key-value store over HTTP.

    Uses the [upstash-redis](https://github.com/upstash/redis-py) async client. Entries are stored
    the same way as `RedisStore` stores them.
    """

    _client: Redis
    _adapter: SerializationAdapter

    @overload
    def __init__(self, *, client: Redis, default_collection: str | None = None) -> None: ...

    @overload
    def __init__(self, *, url: str, token: str, default_collection: str | None = None) -> None: ...

    @bear_spray
    def __init__(
        self,
        *,
        client: Redis | None = None,
        url: str | None = None,
        token: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Upstash Redis store.

        Args:
            client: An existing `upstash_redis.asyncio.Redis` client to use. If provided, the store
                will not manage the client's lifecycle (will not close it).
            url: The Upstash Redis REST URL.
            token: The Upstash Redis REST token.
            default_collection: The default collection to use if no collection is provided.
        """
        client_provided = client is not None

        if client:
            self._client = client
        elif url and token:
            self._client = Redis(url=url, token=token)
        else:
            msg = "Either client or url and token must be provided"
            raise ValueError(msg)

        self._adapter = BasicSerializationAdapter(date_format="isoformat", value_format="dict")

        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided,
        )

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        response = await self._client.get(combo_key)

        if not isinstance(response, str):
            return None

        try:
            return self._adapter.load_json(json_str=response)
        except DeserializationError:
            return None

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        combo_keys: list[str] = [compound_key(collection=collection, key=key) for key in keys]

        responses = await self._client.mget(*combo_keys)

        entries: list[ManagedEntry | None] = []
        for response in responses:
            if isinstance(response, str):
                try:
                    entries.append(self._adapter.load_json(json_str=response))
                except DeserializationError:
                    entries.append(None)
            else:
                entries.append(None)

        return entries

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        json_value: str = self._adapter.dump_json(entry=managed_entry, key=key, collection=collection)

        if managed_entry.ttl is not None:
            # Redis does not support <= 0 TTLs
            ttl = max(int(managed_entry.ttl), 1)

            _ = await self._client.set(combo_key, json_value, ex=ttl)
        else:
            _ = await self._client.set(combo_key, json_value)

    @override
    async def _put_managed_entries(
        self,
        *,
        collection: str,
        keys: Sequence[str],
        managed_entries: Sequence[ManagedEntry],
        ttl: float | None,
        created_at: datetime,
        expires_at: datetime | None,
    ) -> None:
        if not keys:
            return

        if ttl is None:
            # If there is no TTL, we can just do a simple mset
            mapping: dict[str, str] = {}
            for key, managed_entry in zip(keys, managed_entries, strict=True):
                json_value = self._adapter.dump_json(entry=managed_entry, key=key, collection=collection)
                mapping[compound_key(collection=collection, key=key)] = json_value

            _ = await self._client.mset(mapping)

            return

        # Convert TTL to integer seconds for Redis
        ttl_seconds: int = max(int(ttl), 1)

        # Use pipeline for bulk operations
        pipeline = self._client.pipeline()

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            combo_key: str = compound_key(collection=collection, key=key)
            json_value = self._adapter.dump_json(entry=managed_entry, key=key, collection=collection)

            _ = pipeline.set(combo_key, json_value, ex=ttl_seconds)

        _ = await pipeline.exec()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return await self._client.delete(combo_key) != 0

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        if not keys:
            return 0

        combo_keys: list[str] = [compound_key(collection=collection, key=key) for key in keys]

        return await self._client.delete(*combo_keys)

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        pattern = compound_key(collection=collection, key="*")

        cursor = 0
        keys: list[str] = []
        seen: set[str] = set()
        while True:
            # SCAN may return an empty page with a nonzero cursor, or repeat keys.
            cursor, page = await self._client.scan(cursor, match=pattern, count=limit)
            for key in get_keys_from_compound_keys(compound_keys=page, collection=collection):
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
                    if len(keys) >= limit:
                        return keys
            if cursor == 0:
                return keys

    @override
    async def _setup(self) -> None:
        """Register client cleanup if we own the client."""
        if not self._client_provided_by_user:
            self._exit_stack.push_async_callback(self._client.close)

    @override
    async def _delete_store(self) -> bool:
        return await self._client.flushdb()
