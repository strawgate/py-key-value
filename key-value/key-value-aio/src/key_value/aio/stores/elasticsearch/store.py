from typing import TYPE_CHECKING, Any, overload

from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry, load_from_json
from key_value.shared.utils.sanitize import (
    ALPHANUMERIC_CHARACTERS,
    LOWERCASE_ALPHABET,
    NUMBERS,
    sanitize_string,
)
from key_value.shared.utils.time_to_live import now_as_epoch, try_parse_datetime_str
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseCullStore,
    BaseDestroyCollectionStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
    BaseStore,
)

try:
    from elasticsearch import AsyncElasticsearch
    from key_value.aio.stores.elasticsearch.utils import (
        get_aggregations_from_body,
        get_body_from_response,
        get_first_value_from_field_in_hit,
        get_hits_from_response,
        get_source_from_body,
    )
except ImportError as e:
    msg = "ElasticsearchStore requires py-key-value-aio[elasticsearch]"
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from datetime import datetime

    from elastic_transport import ObjectApiResponse

DEFAULT_INDEX_PREFIX = "kv_store"

DEFAULT_MAPPING = {
    "properties": {
        "created_at": {
            "type": "date",
        },
        "expires_at": {
            "type": "date",
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

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

MAX_KEY_LENGTH = 256
ALLOWED_KEY_CHARACTERS: str = ALPHANUMERIC_CHARACTERS

MAX_INDEX_LENGTH = 240
ALLOWED_INDEX_CHARACTERS: str = LOWERCASE_ALPHABET + NUMBERS + "_" + "-" + "."


class ElasticsearchStore(
    BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseDestroyCollectionStore, BaseCullStore, BaseContextManagerStore, BaseStore
):
    """A elasticsearch-based store."""

    _client: AsyncElasticsearch

    _is_serverless: bool

    _index_prefix: str

    @overload
    def __init__(self, *, elasticsearch_client: AsyncElasticsearch, index_prefix: str, default_collection: str | None = None) -> None: ...

    @overload
    def __init__(self, *, url: str, api_key: str | None = None, index_prefix: str, default_collection: str | None = None) -> None: ...

    def __init__(
        self,
        *,
        elasticsearch_client: AsyncElasticsearch | None = None,
        url: str | None = None,
        api_key: str | None = None,
        index_prefix: str,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            elasticsearch_client: The elasticsearch client to use.
            url: The url of the elasticsearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
        """
        if elasticsearch_client is None and url is None:
            msg = "Either elasticsearch_client or url must be provided"
            raise ValueError(msg)

        if elasticsearch_client:
            self._client = elasticsearch_client
        elif url:
            self._client = AsyncElasticsearch(
                hosts=[url], api_key=api_key, http_compress=True, request_timeout=10, retry_on_timeout=True, max_retries=3
            )
        else:
            msg = "Either elasticsearch_client or url must be provided"
            raise ValueError(msg)

        self._index_prefix = index_prefix
        self._is_serverless = False

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        cluster_info = await self._client.options(ignore_status=404).info()

        self._is_serverless = cluster_info.get("version", {}).get("build_flavor") == "serverless"

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        index_name = self._sanitize_index_name(collection=collection)

        if await self._client.options(ignore_status=404).indices.exists(index=index_name):
            return

        _ = await self._client.options(ignore_status=404).indices.create(index=index_name, mappings=DEFAULT_MAPPING, settings={})

    def _sanitize_index_name(self, collection: str) -> str:
        return sanitize_string(
            value=self._index_prefix + "-" + collection,
            replacement_character="_",
            max_length=MAX_INDEX_LENGTH,
            allowed_characters=ALLOWED_INDEX_CHARACTERS,
        )

    def _sanitize_document_id(self, key: str) -> str:
        return sanitize_string(
            value=key,
            replacement_character="_",
            max_length=MAX_KEY_LENGTH,
            allowed_characters=ALLOWED_KEY_CHARACTERS,
        )

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        elasticsearch_response = await self._client.options(ignore_status=404).get(
            index=self._sanitize_index_name(collection=collection), id=self._sanitize_document_id(key=combo_key)
        )

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (source := get_source_from_body(body=body)):
            return None

        if not (value_str := source.get("value")) or not isinstance(value_str, str):
            return None

        created_at: datetime | None = try_parse_datetime_str(value=source.get("created_at"))
        expires_at: datetime | None = try_parse_datetime_str(value=source.get("expires_at"))

        return ManagedEntry(
            value=load_from_json(value_str),
            created_at=created_at,
            expires_at=expires_at,
        )

    @property
    def _should_refresh_on_put(self) -> bool:
        return not self._is_serverless

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        document: dict[str, Any] = {
            "collection": collection,
            "key": key,
            "value": managed_entry.to_json(include_metadata=False),
        }

        if managed_entry.created_at:
            document["created_at"] = managed_entry.created_at.isoformat()
        if managed_entry.expires_at:
            document["expires_at"] = managed_entry.expires_at.isoformat()

        _ = await self._client.index(
            index=self._sanitize_index_name(collection=collection),
            id=self._sanitize_document_id(key=combo_key),
            body=document,
            refresh=self._should_refresh_on_put,
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        elasticsearch_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete(
            index=self._sanitize_index_name(collection=collection), id=self._sanitize_document_id(key=combo_key)
        )

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (result := body.get("result")) or not isinstance(result, str):
            return False

        return result == "deleted"

    @override
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        """Get up to 10,000 keys in the specified collection (eventually consistent)."""

        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=self._sanitize_index_name(collection=collection),
            fields=[{"key": None}],
            body={
                "query": {
                    "term": {
                        "collection": collection,
                    },
                },
            },
            source_includes=[],
            size=limit,
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
    async def _get_collection_names(self, *, limit: int | None = None) -> list[str]:
        """List up to 10,000 collections in the elasticsearch store (eventually consistent)."""

        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        search_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).search(
            index=f"{self._index_prefix}-*",
            aggregations={
                "collections": {
                    "terms": {
                        "field": "collection",
                        "size": limit,
                    },
                },
            },
            size=limit,
        )

        body: dict[str, Any] = get_body_from_response(response=search_response)
        aggregations: dict[str, Any] = get_aggregations_from_body(body=body)

        buckets: list[Any] = aggregations["collections"]["buckets"]  # pyright: ignore[reportAny]

        return [bucket["key"] for bucket in buckets]  # pyright: ignore[reportAny]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        result: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete_by_query(
            index=self._sanitize_index_name(collection=collection),
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
            return False

        return deleted > 0

    @override
    async def _cull(self) -> None:
        ms_epoch = int(now_as_epoch() * 1000)
        _ = await self._client.options(ignore_status=404).delete_by_query(
            index=f"{self._index_prefix}-*",
            body={
                "query": {
                    "range": {
                        "expires_at": {"lt": ms_epoch},
                    },
                },
            },
        )

    @override
    async def _close(self) -> None:
        await self._client.close()
