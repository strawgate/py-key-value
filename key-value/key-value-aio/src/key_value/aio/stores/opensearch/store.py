import contextlib
import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any, overload

from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitization import (
    AlwaysHashStrategy,
    HashFragmentMode,
    HybridSanitizationStrategy,
    SanitizationStrategy,
)
from key_value.shared.utils.sanitize import (
    ALPHANUMERIC_CHARACTERS,
    LOWERCASE_ALPHABET,
    NUMBERS,
    UPPERCASE_ALPHABET,
)
from key_value.shared.utils.serialization import SerializationAdapter
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
from key_value.aio.stores.opensearch.utils import LessCapableJsonSerializer, new_bulk_action

try:
    from opensearchpy import AsyncOpenSearch
    from opensearchpy.exceptions import RequestError

    from key_value.aio.stores.opensearch.utils import (
        get_aggregations_from_body,
        get_body_from_response,
        get_first_value_from_field_in_hit,
        get_hits_from_response,
        get_source_from_body,
    )
except ImportError as e:
    msg = "OpenSearchStore requires opensearch-py[async]>=2.0.0. Install with: pip install 'py-key-value-aio[opensearch]'"
    raise ImportError(msg) from e


logger = logging.getLogger(__name__)

DEFAULT_INDEX_PREFIX = "opensearch_kv_store"

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
                "flat": {
                    "type": "flat_object",
                },
            },
        },
    },
}

DEFAULT_PAGE_SIZE = 10000
PAGE_LIMIT = 10000

MAX_KEY_LENGTH = 256
ALLOWED_KEY_CHARACTERS: str = ALPHANUMERIC_CHARACTERS

MAX_INDEX_LENGTH = 200
ALLOWED_INDEX_CHARACTERS: str = LOWERCASE_ALPHABET + NUMBERS + "_" + "-" + "."


class OpenSearchSerializationAdapter(SerializationAdapter):
    """Adapter for OpenSearch."""

    def __init__(self) -> None:
        """Initialize the OpenSearch adapter"""
        super().__init__()

        self._date_format = "isoformat"
        self._value_format = "dict"

    @override
    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        value = data.pop("value")

        data["value"] = {
            "flat": value,
        }

        return data

    @override
    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        data["value"] = data.pop("value").get("flat")

        return data


class OpenSearchV1KeySanitizationStrategy(AlwaysHashStrategy):
    def __init__(self) -> None:
        super().__init__(
            hash_length=64,
        )


class OpenSearchV1CollectionSanitizationStrategy(HybridSanitizationStrategy):
    def __init__(self) -> None:
        super().__init__(
            replacement_character="_",
            max_length=MAX_INDEX_LENGTH,
            allowed_characters=UPPERCASE_ALPHABET + ALLOWED_INDEX_CHARACTERS,
            hash_fragment_mode=HashFragmentMode.ALWAYS,
        )


class OpenSearchStore(
    BaseEnumerateCollectionsStore, BaseEnumerateKeysStore, BaseDestroyCollectionStore, BaseCullStore, BaseContextManagerStore, BaseStore
):
    """An OpenSearch-based store.

    Stores collections in their own indices and stores values in Flattened fields.

    This store has specific restrictions on what is allowed in keys and collections. Keys and collections are not sanitized
    by default which may result in errors when using the store.

    To avoid issues, you may want to consider leveraging the `OpenSearchV1KeySanitizationStrategy` and
    `OpenSearchV1CollectionSanitizationStrategy` strategies.
    """

    _client: AsyncOpenSearch

    _index_prefix: str

    _default_collection: str | None

    _serializer: SerializationAdapter

    _key_sanitization_strategy: SanitizationStrategy
    _collection_sanitization_strategy: SanitizationStrategy

    @overload
    def __init__(
        self,
        *,
        opensearch_client: AsyncOpenSearch,
        index_prefix: str,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the opensearch store.

        Args:
            opensearch_client: The opensearch client to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

    @overload
    def __init__(
        self,
        *,
        url: str,
        api_key: str | None = None,
        index_prefix: str,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the opensearch store.

        Args:
            url: The url of the opensearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        opensearch_client: AsyncOpenSearch | None = None,
        url: str | None = None,
        api_key: str | None = None,
        index_prefix: str,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the opensearch store.

        Args:
            opensearch_client: The opensearch client to use.
            url: The url of the opensearch cluster.
            api_key: The api key to use.
            index_prefix: The index prefix to use. Collections will be prefixed with this prefix.
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """
        if opensearch_client is None and url is None:
            msg = "Either opensearch_client or url must be provided"
            raise ValueError(msg)

        if opensearch_client:
            self._client = opensearch_client
        elif url:
            client_kwargs: dict[str, Any] = {
                "hosts": [url],
                "http_compress": True,
                "timeout": 10,
                "max_retries": 3,
            }
            if api_key:
                client_kwargs["api_key"] = api_key

            self._client = AsyncOpenSearch(**client_kwargs)
        else:
            msg = "Either opensearch_client or url must be provided"
            raise ValueError(msg)

        LessCapableJsonSerializer.install_serializer(client=self._client)

        self._index_prefix = index_prefix.lower()

        self._serializer = OpenSearchSerializationAdapter()

        super().__init__(
            default_collection=default_collection,
            collection_sanitization_strategy=collection_sanitization_strategy,
            key_sanitization_strategy=key_sanitization_strategy,
        )

    @override
    async def _setup(self) -> None:
        # OpenSearch doesn't have serverless mode, so we can skip the cluster info check
        pass

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        index_name = self._get_index_name(collection=collection)

        if await self._client.indices.exists(index=index_name):
            return

        try:
            _ = await self._client.indices.create(index=index_name, body={"mappings": DEFAULT_MAPPING, "settings": {}})
        except RequestError as e:
            if "resource_already_exists_exception" in str(e).lower():
                return
            raise

    def _get_index_name(self, collection: str) -> str:
        return self._index_prefix + "-" + self._sanitize_collection(collection=collection).lower()

    def _get_document_id(self, key: str) -> str:
        return self._sanitize_key(key=key)

    def _get_destination(self, *, collection: str, key: str) -> tuple[str, str]:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        return index_name, document_id

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        index_name, document_id = self._get_destination(collection=collection, key=key)

        try:
            opensearch_response = await self._client.get(index=index_name, id=document_id)
        except Exception:
            return None

        body: dict[str, Any] = get_body_from_response(response=opensearch_response)

        if not (source := get_source_from_body(body=body)):
            return None

        try:
            return self._serializer.load_dict(data=source)
        except DeserializationError:
            return None

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        # Use mget for efficient batch retrieval
        index_name = self._get_index_name(collection=collection)
        document_ids = [self._get_document_id(key=key) for key in keys]
        docs = [{"_id": document_id} for document_id in document_ids]

        try:
            opensearch_response = await self._client.mget(index=index_name, body={"docs": docs})
        except Exception:
            return [None] * len(keys)

        body: dict[str, Any] = get_body_from_response(response=opensearch_response)
        docs_result = body.get("docs", [])

        entries_by_id: dict[str, ManagedEntry | None] = {}
        for doc in docs_result:
            if not (doc_id := doc.get("_id")):
                continue

            if "found" not in doc or not doc.get("found"):
                entries_by_id[doc_id] = None
                continue

            if not (source := doc.get("_source")):
                entries_by_id[doc_id] = None
                continue

            try:
                entries_by_id[doc_id] = self._serializer.load_dict(data=source)
            except DeserializationError as e:
                logger.error(
                    "Failed to deserialize OpenSearch document in batch operation",
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

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        document: dict[str, Any] = self._serializer.dump_dict(entry=managed_entry, key=key, collection=collection)

        try:
            _ = await self._client.index(  # type: ignore[reportUnknownVariableType]
                index=index_name,
                id=document_id,
                body=document,
                params={"refresh": "true"},
            )
        except Exception as e:
            msg = f"Failed to serialize document: {e}"
            raise SerializationError(message=msg) from e

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

        operations: list[dict[str, Any] | str] = []

        index_name: str = self._get_index_name(collection=collection)

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            document_id: str = self._get_document_id(key=key)

            index_action: dict[str, Any] = new_bulk_action(action="index", index=index_name, document_id=document_id)

            document: dict[str, Any] = self._serializer.dump_dict(entry=managed_entry, key=key, collection=collection)

            operations.extend([index_action, document])

        try:
            _ = await self._client.bulk(body=operations, params={"refresh": "true"})  # type: ignore[reportUnknownVariableType]
        except Exception as e:
            msg = f"Failed to serialize bulk operations: {e}"
            raise SerializationError(message=msg) from e

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        index_name: str = self._get_index_name(collection=collection)
        document_id: str = self._get_document_id(key=key)

        try:
            opensearch_response = await self._client.delete(index=index_name, id=document_id)
        except Exception:
            return False

        body: dict[str, Any] = get_body_from_response(response=opensearch_response)

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

        try:
            opensearch_response = await self._client.bulk(body=operations)
        except Exception:
            return 0

        body: dict[str, Any] = get_body_from_response(response=opensearch_response)

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

        try:
            result = await self._client.search(
                index=self._get_index_name(collection=collection),
                body={
                    "query": {
                        "term": {
                            "collection": collection,
                        },
                    },
                    "_source": False,
                    "fields": ["key"],
                    "size": limit,
                },
            )
        except Exception:
            return []

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
        """List up to 10,000 collections in the opensearch store (eventually consistent)."""

        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        try:
            search_response = await self._client.search(
                index=f"{self._index_prefix}-*",
                body={
                    "aggs": {
                        "collections": {
                            "terms": {
                                "field": "collection",
                                "size": limit,
                            },
                        },
                    },
                    "size": 0,
                },
            )
        except Exception:
            return []

        body: dict[str, Any] = get_body_from_response(response=search_response)
        aggregations: dict[str, Any] = get_aggregations_from_body(body=body)

        if not aggregations or "collections" not in aggregations:
            return []

        buckets: list[Any] = aggregations["collections"].get("buckets", [])

        return [bucket["key"] for bucket in buckets if isinstance(bucket, dict) and "key" in bucket]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        try:
            result = await self._client.delete_by_query(
                index=self._get_index_name(collection=collection),
                body={
                    "query": {
                        "term": {
                            "collection": collection,
                        },
                    },
                },
            )
        except Exception:
            return False

        body: dict[str, Any] = get_body_from_response(response=result)

        if not (deleted := body.get("deleted")) or not isinstance(deleted, int):
            return False

        return deleted > 0

    @override
    async def _cull(self) -> None:
        ms_epoch = int(now_as_epoch() * 1000)
        with contextlib.suppress(Exception):
            _ = await self._client.delete_by_query(
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
