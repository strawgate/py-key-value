from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, overload

from elasticsearch import AsyncElasticsearch
from typing_extensions import override

from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.elasticsearch.utils import (
    get_aggregations_from_body,
    get_body_from_response,
    get_first_value_from_field_in_hit,
    get_hits_from_response,
    get_source_from_body,
)
from kv_store_adapter.stores.utils.compound import compound_key
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry, dump_to_json, load_from_json

if TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

DEFAULT_DISK_STORE_SIZE_LIMIT = 1 * 1024 * 1024 * 1024  # 1GB

ELASTICSEARCH_CLIENT_DEFAULTS = {
    "http_compress": True,
    "timeout": 10,
    "retry_on_timeout": True,
    "max_retries": 3,
}

DEFAULT_INDEX = "kv-store"

DEFAULT_MAPPING = {
    "properties": {
        "created_at": {
            "type": "date",
        },
        "expires_at": {
            "type": "date",
        },
        "ttl": {
            "type": "float",
        },
        "collection": {
            "type": "keyword",
        },
        "key": {
            "type": "keyword",
        },
        "value": {
            "type": "keyword",
            "index": False,
            "doc_values": False,
            "ignore_above": 256,
        },
    },
}


class ElasticsearchStore(BaseManagedKVStore):
    """A elasticsearch-based store."""

    _client: AsyncElasticsearch

    _index: str

    @overload
    def __init__(self, *, elasticsearch_client: AsyncElasticsearch, index: str) -> None: ...

    @overload
    def __init__(self, *, url: str, api_key: str, index: str) -> None: ...

    def __init__(
        self, *, elasticsearch_client: AsyncElasticsearch | None = None, url: str | None = None, api_key: str | None = None, index: str
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            elasticsearch_client: The elasticsearch client to use.
            url: The url of the elasticsearch cluster.
            api_key: The api key to use.
            index: The index to use. Defaults to "kv-store".
        """
        self._client = elasticsearch_client or AsyncElasticsearch(hosts=[url], api_key=api_key, **ELASTICSEARCH_CLIENT_DEFAULTS)  # pyright: ignore[reportArgumentType]
        self._index = index or DEFAULT_INDEX
        super().__init__()

    @override
    async def setup(self) -> None:
        if await self._client.options(ignore_status=404).indices.exists(index=self._index):
            return

        _ = await self._client.options(ignore_status=404).indices.create(
            index=self._index,
            mappings=DEFAULT_MAPPING,
        )

    @override
    async def setup_collection(self, collection: str) -> None:
        pass

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        elasticsearch_response = await self._client.options(ignore_status=404).get(index=self._index, id=combo_key)

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (source := get_source_from_body(body=body)):
            return None

        if not (value_str := source.get("value")) or not isinstance(value_str, str):
            return None

        if not (created_at := source.get("created_at")) or not isinstance(created_at, str):
            return None

        ttl: Any | float | int | None = source.get("ttl")
        expires_at: Any | str | None = source.get("expires_at")

        if not isinstance(ttl, float | int | None):
            return None

        if not isinstance(expires_at, str | None):
            return None

        return ManagedEntry(
            collection=collection,
            key=key,
            value=load_from_json(value_str),
            created_at=datetime.fromisoformat(created_at),
            ttl=float(ttl) if ttl else None,
            expires_at=datetime.fromisoformat(expires_at) if expires_at else None,
        )

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

        _ = await self._client.index(
            index=self._index,
            id=combo_key,
            body={
                "collection": collection,
                "key": key,
                "value": dump_to_json(cache_entry.value),
                "created_at": cache_entry.created_at.isoformat() if cache_entry.created_at else None,
                "expires_at": cache_entry.expires_at.isoformat() if cache_entry.expires_at else None,
                "ttl": cache_entry.ttl,
            },
        )

    @override
    async def delete(self, collection: str, key: str) -> bool:
        await self.setup_collection_once(collection=collection)

        combo_key: str = compound_key(collection=collection, key=key)
        elasticsearch_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete(
            index=self._index, id=combo_key
        )

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (result := body.get("result")) or not isinstance(result, str):
            return False

        return result == "deleted"

    @override
    async def keys(self, collection: str) -> list[str]:
        """Get up to 10,000 keys in the specified collection (eventually consistent)."""
        await self.setup_collection_once(collection=collection)

        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=self._index,
            fields=["key"],  # pyright: ignore[reportArgumentType]
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
            source_includes=[],
            size=10000,
        )

        if not (hits := get_hits_from_response(response=result)):
            return []

        all_keys: list[str] = []

        for hit in hits:
            if not (key := get_first_value_from_field_in_hit(hit=hit, field="key", value_type=str)):
                continue

            all_keys.append(key)

        return all_keys

    @override
    async def clear_collection(self, collection: str) -> int:
        await self.setup_collection_once(collection=collection)

        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete_by_query(
            index=self._index,
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
        )

        body: dict[str, Any] = get_body_from_response(response=result)

        if not (deleted := body.get("deleted")) or not isinstance(deleted, int):
            return 0

        return deleted

    @override
    async def list_collections(self) -> list[str]:
        """List up to 10,000 collections in the elasticsearch store (eventually consistent)."""
        await self.setup_once()

        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=self._index,
            aggregations={
                "collections": {
                    "terms": {
                        "field": "collection",
                    },
                },
            },
            size=10000,
        )

        body: dict[str, Any] = get_body_from_response(response=result)
        aggregations: dict[str, Any] = get_aggregations_from_body(body=body)

        buckets: list[Any] = aggregations["collections"]["buckets"]  # pyright: ignore[reportAny]

        return [bucket["key"] for bucket in buckets]  # pyright: ignore[reportAny]

    @override
    async def cull(self) -> None:
        await self.setup_once()

        _ = await self._client.options(ignore_status=404).delete_by_query(
            index=self._index,
            body={
                "query": {
                    "range": {
                        "expires_at": {"lt": datetime.now(tz=timezone.utc).timestamp()},
                    },
                },
            },
        )
