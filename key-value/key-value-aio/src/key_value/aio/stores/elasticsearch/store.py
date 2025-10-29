import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any, overload

from elastic_transport import ObjectApiResponse
from elastic_transport import SerializationError as ElasticsearchSerializationError
from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import (
    ALPHANUMERIC_CHARACTERS,
    LOWERCASE_ALPHABET,
    NUMBERS,
    sanitize_string,
)
from key_value.shared.utils.serialization import ElasticsearchAdapter, SerializationAdapter
from key_value.shared.utils.time_to_live import now_as_epoch
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseCullStore,
    BaseDestroyCollectionStore,
    BaseEnumerateCollectionsStore,
    BaseEnumerateKeysStore,
    BaseStore,
)
from key_value.aio.stores.elasticsearch.utils import LessCapableJsonSerializer, LessCapableNdjsonSerializer, new_bulk_action

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


logger = logging.getLogger(__name__)

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
            "properties": {
                # You might think the `string` field should be a text/keyword field
                # but this is the recommended mapping for large stringified json
                "string": {
                    "type": "object",
                    "enabled": False,
                },
                "flattened": {
                    "type": "flattened",
                },
            },
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

    _native_storage: bool

    _adapter: SerializationAdapter

    @overload
    def __init__(
        self,
        *,
        elasticsearch_client: AsyncElasticsearch,
        index_prefix: str,
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        url: str,
        api_key: str | None = None,
        index_prefix: str,
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None: ...

    def __init__(
        self,
        *,
        elasticsearch_client: AsyncElasticsearch | None = None,
        url: str | None = None,
        api_key: str | None = None,
        index_prefix: str,
        native_storage: bool = True,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the elasticsearch store.

        Args:
            elasticsearch_client: The elasticsearch client to use.
            url: The url of the elasticsearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            native_storage: Whether to use native storage mode (flattened field type) or serialize
                            all values to JSON strings. Defaults to True.
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

        LessCapableJsonSerializer.install_serializer(client=self._client)
        LessCapableJsonSerializer.install_default_serializer(client=self._client)
        LessCapableNdjsonSerializer.install_serializer(client=self._client)

        self._index_prefix = index_prefix
        self._native_storage = native_storage
        self._is_serverless = False
        self._adapter = ElasticsearchAdapter(native_storage=native_storage)

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

    def _get_destination(self, *, collection: str, key: str) -> tuple[str, str]:
        index_name: str = self._sanitize_index_name(collection=collection)
        document_id: str = self._sanitize_document_id(key=key)

        return index_name, document_id

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        index_name, document_id = self._get_destination(collection=collection, key=key)

        elasticsearch_response = await self._client.options(ignore_status=404).get(index=index_name, id=document_id)

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (source := get_source_from_body(body=body)):
            return None

        try:
            return self._adapter.from_storage(data=source)
        except DeserializationError:
            return None

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        # Use mget for efficient batch retrieval
        index_name = self._sanitize_index_name(collection=collection)
        document_ids = [self._sanitize_document_id(key=key) for key in keys]
        docs = [{"_id": document_id} for document_id in document_ids]

        elasticsearch_response = await self._client.options(ignore_status=404).mget(index=index_name, docs=docs)

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)
        docs_result = body.get("docs", [])

        entries_by_id: dict[str, ManagedEntry | None] = {}
        for doc in docs_result:
            if not (doc_id := doc.get("_id")):
                continue

            if "found" not in doc:
                entries_by_id[doc_id] = None
                continue

            if not (source := doc.get("_source")):
                entries_by_id[doc_id] = None
                continue

            try:
                entries_by_id[doc_id] = self._adapter.from_storage(data=source)
            except DeserializationError as e:
                logger.error(
                    "Failed to deserialize Elasticsearch document in batch operation",
                    extra={
                        "collection": collection,
                        "document_id": doc_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                entries_by_id[doc_id] = None

        # Return entries in the same order as input keys
        return [entries_by_id.get(document_id) for document_id in document_ids]

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
        index_name: str = self._sanitize_index_name(collection=collection)
        document_id: str = self._sanitize_document_id(key=key)

        document: dict[str, Any] = self._adapter.to_storage(key=key, entry=managed_entry, collection=collection)
        if not isinstance(document, dict):
            msg = "Elasticsearch adapter must return dict"
            raise TypeError(msg)

        try:
            _ = await self._client.index(
                index=index_name,
                id=document_id,
                body=document,
                refresh=self._should_refresh_on_put,
            )
        except ElasticsearchSerializationError as e:
            msg = f"Failed to serialize document: {e}"
            raise SerializationError(message=msg) from e
        except Exception:
            raise

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

        operations: list[dict[str, Any]] = []

        index_name: str = self._sanitize_index_name(collection=collection)

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            document_id: str = self._sanitize_document_id(key=key)

            index_action: dict[str, Any] = new_bulk_action(action="index", index=index_name, document_id=document_id)

            document: dict[str, Any] = self._adapter.to_storage(key=key, entry=managed_entry, collection=collection)
            if not isinstance(document, dict):
                msg = "Elasticsearch adapter must return dict"
                raise TypeError(msg)

            operations.extend([index_action, document])
        try:
            _ = await self._client.bulk(operations=operations, refresh=self._should_refresh_on_put)  # pyright: ignore[reportUnknownMemberType]
        except ElasticsearchSerializationError as e:
            msg = f"Failed to serialize bulk operations: {e}"
            raise SerializationError(message=msg) from e
        except Exception:
            raise

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        index_name: str = self._sanitize_index_name(collection=collection)
        document_id: str = self._sanitize_document_id(key=key)

        elasticsearch_response: ObjectApiResponse[Any] = await self._client.options(ignore_status=404).delete(
            index=index_name, id=document_id
        )

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        if not (result := body.get("result")) or not isinstance(result, str):
            return False

        return result == "deleted"

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        if not keys:
            return 0

        operations: list[dict[str, Any]] = []

        for key in keys:
            index_name, document_id = self._get_destination(collection=collection, key=key)

            delete_action: dict[str, Any] = new_bulk_action(action="delete", index=index_name, document_id=document_id)

            operations.append(delete_action)

        elasticsearch_response = await self._client.bulk(operations=operations)  # pyright: ignore[reportUnknownMemberType]

        body: dict[str, Any] = get_body_from_response(response=elasticsearch_response)

        # Count successful deletions
        deleted_count = 0
        items = body.get("items", [])
        for item in items:
            delete_result = item.get("delete", {})
            if delete_result.get("result") == "deleted":
                deleted_count += 1

        return deleted_count

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
