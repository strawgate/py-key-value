from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, overload

from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseDestroyCollectionStore, BaseStore
from key_value.shared.errors import DeserializationError, SerializationError
from key_value.shared.managed_entry import ManagedEntry
from key_value.shared.sanitization import HybridSanitizationStrategy, SanitizationStrategy
from key_value.shared.sanitize import ALPHANUMERIC_CHARACTERS
from key_value.shared.serialization import SerializationAdapter

try:
    from acouchbase.cluster import Cluster as AsyncCluster
    from couchbase.auth import PasswordAuthenticator
    from couchbase.exceptions import DocumentNotFoundException
    from couchbase.options import ClusterOptions, GetOptions, UpsertOptions
except ImportError as e:
    msg = "CouchbaseStore requires py-key-value-aio[couchbase]"
    raise ImportError(msg) from e


DEFAULT_BUCKET = "default"
DEFAULT_SCOPE = "_default"
DEFAULT_COLLECTION = "_default"

# Couchbase collection name length limit (maximum 251 bytes)
MAX_COLLECTION_LENGTH = 200
COLLECTION_ALLOWED_CHARACTERS = ALPHANUMERIC_CHARACTERS + "_-%"


class CouchbaseSerializationAdapter(SerializationAdapter):
    """Adapter for Couchbase with native JSON storage."""

    def __init__(self) -> None:
        """Initialize the Couchbase adapter."""
        super().__init__()

        self._date_format = "datetime"
        self._value_format = "dict"

    @override
    def prepare_dump(self, data: dict[str, Any]) -> dict[str, Any]:
        value = data.pop("value")

        data["value"] = {"object": value}

        return data

    @override
    def prepare_load(self, data: dict[str, Any]) -> dict[str, Any]:
        value = data.pop("value")

        if "object" in value:
            data["value"] = value["object"]
        else:
            msg = "Value field not found in Couchbase document"
            raise DeserializationError(message=msg)

        if date_created := data.get("created_at"):
            if isinstance(date_created, str):
                # Parse ISO format datetime string
                data["created_at"] = datetime.fromisoformat(date_created.replace("Z", "+00:00"))
            elif not isinstance(date_created, datetime):
                msg = "Expected `created_at` field to be a datetime or ISO string"
                raise DeserializationError(message=msg)
            else:
                data["created_at"] = date_created.replace(tzinfo=timezone.utc)

        if date_expires := data.get("expires_at"):
            if isinstance(date_expires, str):
                # Parse ISO format datetime string
                data["expires_at"] = datetime.fromisoformat(date_expires.replace("Z", "+00:00"))
            elif not isinstance(date_expires, datetime):
                msg = "Expected `expires_at` field to be a datetime or ISO string"
                raise DeserializationError(message=msg)
            else:
                data["expires_at"] = date_expires.replace(tzinfo=timezone.utc)

        return data


class CouchbaseV1CollectionSanitizationStrategy(HybridSanitizationStrategy):
    def __init__(self) -> None:
        super().__init__(
            replacement_character="_",
            max_length=MAX_COLLECTION_LENGTH,
            allowed_characters=COLLECTION_ALLOWED_CHARACTERS,
        )


class CouchbaseStore(BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """Couchbase-based key-value store using the acouchbase async SDK.

    Stores collections as Couchbase collections within a scope and stores values as JSON documents.

    By default, collections are not sanitized. This means that there are character and length restrictions on
    collection names that may cause errors when trying to get and put entries.

    To avoid issues, you may want to consider leveraging the `CouchbaseV1CollectionSanitizationStrategy` strategy.

    Note: This store uses a single Couchbase bucket and scope, mapping py-key-value collections to
    Couchbase collections within that scope. TTL is supported via Couchbase's built-in document expiration.
    """

    _cluster: AsyncCluster
    _bucket_name: str
    _scope_name: str
    _collections_by_name: dict[str, Any]  # Any due to acouchbase's incomplete type stubs
    _adapter: SerializationAdapter

    @overload
    def __init__(
        self,
        *,
        cluster: AsyncCluster,
        bucket_name: str | None = None,
        scope_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Couchbase store.

        Args:
            cluster: The async Couchbase cluster to use.
            bucket_name: The name of the Couchbase bucket.
            scope_name: The name of the Couchbase scope.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

    @overload
    def __init__(
        self,
        *,
        connection_string: str,
        username: str,
        password: str,
        bucket_name: str | None = None,
        scope_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Couchbase store.

        Args:
            connection_string: The Couchbase connection string (e.g., "couchbase://localhost").
            username: The username for authentication.
            password: The password for authentication.
            bucket_name: The name of the Couchbase bucket.
            scope_name: The name of the Couchbase scope.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

    def __init__(
        self,
        *,
        cluster: AsyncCluster | None = None,
        connection_string: str | None = None,
        username: str | None = None,
        password: str | None = None,
        bucket_name: str | None = None,
        scope_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Couchbase store.

        Values are stored as native JSON documents for better query support and performance.

        Args:
            cluster: The async Couchbase cluster to use (mutually exclusive with connection_string).
                If provided, the store will not manage the cluster's lifecycle.
                The caller is responsible for managing the cluster's lifecycle.
            connection_string: The Couchbase connection string (mutually exclusive with cluster).
            username: The username for authentication (required if connection_string is provided).
            password: The password for authentication (required if connection_string is provided).
            bucket_name: The name of the Couchbase bucket.
            scope_name: The name of the Couchbase scope.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

        cluster_provided = cluster is not None

        if cluster:
            self._cluster = cluster
        else:
            if not connection_string:
                msg = "Either cluster or connection_string must be provided"
                raise ValueError(msg)
            if not username or not password:
                msg = "username and password are required when using connection_string"
                raise ValueError(msg)

            auth = PasswordAuthenticator(username, password)
            self._cluster = AsyncCluster(connection_string, ClusterOptions(auth))

        self._bucket_name = bucket_name or DEFAULT_BUCKET
        self._scope_name = scope_name or DEFAULT_SCOPE
        self._collections_by_name = {}
        self._adapter = CouchbaseSerializationAdapter()

        super().__init__(
            default_collection=default_collection,
            collection_sanitization_strategy=collection_sanitization_strategy,
            client_provided_by_user=cluster_provided,
        )

    @override
    async def _setup(self) -> None:
        """Initialize the cluster connection and register cleanup."""
        # Wait for cluster connection
        bucket = self._cluster.bucket(self._bucket_name)  # pyright: ignore[reportUnknownMemberType]
        await bucket.on_connect()  # pyright: ignore[reportUnknownMemberType]

        # Register cleanup if we own the cluster
        if not self._client_provided_by_user:
            self._exit_stack.callback(self._cluster.close)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        """Set up a collection reference."""
        sanitized_collection = self._sanitize_collection(collection=collection)

        # Get the bucket, scope, and collection
        bucket = self._cluster.bucket(self._bucket_name)  # pyright: ignore[reportUnknownMemberType]
        scope = bucket.scope(self._scope_name)

        # Use the sanitized collection name or _default if using default collection
        coll_name = sanitized_collection if sanitized_collection else DEFAULT_COLLECTION
        coll = scope.collection(coll_name)

        self._collections_by_name[collection] = coll

    def _build_document_key(self, *, key: str, collection: str) -> str:
        """Build the full document key including collection prefix.

        Since we're using the _default collection for simplicity, we prefix keys with
        the collection name to separate different py-key-value collections.
        """
        sanitized_collection = self._sanitize_collection(collection=collection)
        return f"{sanitized_collection}::{key}"

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        coll = self._collections_by_name[collection]
        doc_key = self._build_document_key(key=key, collection=collection)

        try:
            result = await coll.get(doc_key, GetOptions(with_expiry=True))
            doc: dict[str, Any] = result.content_as[dict]

            try:
                return self._adapter.load_dict(data=doc)
            except DeserializationError:
                return None
        except DocumentNotFoundException:
            return None

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        if not keys:
            return []

        results: list[ManagedEntry | None] = []
        coll = self._collections_by_name[collection]

        for key in keys:
            doc_key = self._build_document_key(key=key, collection=collection)
            try:
                result = await coll.get(doc_key)
                doc: dict[str, Any] = result.content_as[dict]
                try:
                    results.append(self._adapter.load_dict(data=doc))
                except DeserializationError:
                    results.append(None)
            except DocumentNotFoundException:
                results.append(None)

        return results

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        coll = self._collections_by_name[collection]
        doc_key = self._build_document_key(key=key, collection=collection)
        doc = self._adapter.dump_dict(entry=managed_entry, key=key, collection=collection)

        try:
            # Ensure that the value is serializable to JSON
            _ = managed_entry.value_as_json

            # Calculate expiry timedelta if expires_at is set
            opts: UpsertOptions | None = None
            if managed_entry.expires_at:
                # Calculate the TTL from now to expiration
                ttl_seconds = managed_entry.ttl
                if ttl_seconds is not None and ttl_seconds > 0:
                    opts = UpsertOptions(expiry=timedelta(seconds=ttl_seconds))

            if opts:
                await coll.upsert(doc_key, doc, opts)
            else:
                await coll.upsert(doc_key, doc)
        except Exception as e:
            msg = f"Failed to upsert Couchbase document: {e}"
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

        coll = self._collections_by_name[collection]

        # Calculate expiry options once since all entries share the same TTL
        opts: UpsertOptions | None = None
        if ttl is not None and ttl > 0:
            opts = UpsertOptions(expiry=timedelta(seconds=ttl))

        for key, managed_entry in zip(keys, managed_entries, strict=True):
            doc_key = self._build_document_key(key=key, collection=collection)
            doc = self._adapter.dump_dict(entry=managed_entry, key=key, collection=collection)

            try:
                if opts:
                    await coll.upsert(doc_key, doc, opts)
                else:
                    await coll.upsert(doc_key, doc)
            except Exception as e:
                msg = f"Failed to upsert Couchbase document: {e}"
                raise SerializationError(message=msg) from e

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        coll = self._collections_by_name[collection]
        doc_key = self._build_document_key(key=key, collection=collection)

        try:
            await coll.remove(doc_key)
        except DocumentNotFoundException:
            return False
        else:
            return True

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        if not keys:
            return 0

        deleted_count = 0
        coll = self._collections_by_name[collection]

        for key in keys:
            doc_key = self._build_document_key(key=key, collection=collection)
            try:
                await coll.remove(doc_key)
                deleted_count += 1
            except DocumentNotFoundException:
                pass

        return deleted_count

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete all documents in the collection.

        Note: This doesn't delete the Couchbase collection itself, just removes all
        documents with keys matching the collection prefix.
        """
        # Since we prefix keys with collection name, we can't easily delete all
        # documents without a query. For now, just clear the reference.
        self._collections_by_name.pop(collection, None)

        return True

    # No need to override close - the exit stack handles all cleanup automatically
