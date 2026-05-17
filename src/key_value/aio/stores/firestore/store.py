import hashlib
import re
from collections.abc import Sequence
from datetime import datetime
from typing import overload

from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio._utils.sanitization import SanitizationStrategy
from key_value.aio.errors import InvalidKeyError
from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
    BasicSerializationAdapter,
)

try:
    from google.auth.credentials import Credentials
    from google.cloud import firestore
except ImportError as e:
    msg = "FirestoreStore requires the `firestore` extra"
    raise ImportError(msg) from e


MAX_FIRESTORE_ID_BYTES = 1500
FIRESTORE_HASH_LENGTH = 64
FIRESTORE_HASH_PREFIX = "H_"
FIRESTORE_SANITIZED_PREFIX = "S_"
FIRESTORE_SANITIZED_HASH_LENGTH = 64
FIRESTORE_RESERVED_ID_RE = re.compile(r"__.*__")


def _utf8_truncate(value: str, max_bytes: int) -> str:
    """Truncate a string to a UTF-8 byte limit without splitting codepoints."""
    encoded = value.encode("utf-8")[:max_bytes]
    return encoded.decode("utf-8", errors="ignore")


def _requires_firestore_id_sanitization(value: str) -> bool:
    """Return True when a value violates Firestore collection/document ID rules."""
    return value == "" or "/" in value or value in (".", "..") or FIRESTORE_RESERVED_ID_RE.fullmatch(value) is not None


class FirestoreV1KeySanitizationStrategy(SanitizationStrategy):
    """Sanitize Firestore document IDs.

    Firestore IDs must be UTF-8, no longer than 1500 bytes, cannot contain
    ``/``, cannot be ``.`` or ``..``, and cannot match ``__.*__``.
    """

    def sanitize(self, value: str) -> str:
        """Sanitize a key for use as a Firestore document ID."""
        changed = _requires_firestore_id_sanitization(value)
        if len(value.encode("utf-8")) > MAX_FIRESTORE_ID_BYTES:
            sha256_hash = hashlib.sha256(value.encode()).hexdigest()
            return f"{FIRESTORE_HASH_PREFIX}{sha256_hash[:FIRESTORE_HASH_LENGTH]}"

        if not changed:
            return value

        sanitized = value.replace("/", "_")
        sha256_hash = hashlib.sha256(value.encode()).hexdigest()[:FIRESTORE_SANITIZED_HASH_LENGTH]
        overhead = len(FIRESTORE_SANITIZED_PREFIX) + 1 + FIRESTORE_SANITIZED_HASH_LENGTH
        sanitized = _utf8_truncate(sanitized, MAX_FIRESTORE_ID_BYTES - overhead)
        return f"{FIRESTORE_SANITIZED_PREFIX}{sanitized}-{sha256_hash}"

    def validate(self, value: str) -> None:
        """Validate that the user value does not use reserved sanitizer prefixes."""
        if value.startswith((FIRESTORE_HASH_PREFIX, FIRESTORE_SANITIZED_PREFIX)):
            msg = f"Firestore IDs cannot start with reserved prefixes '{FIRESTORE_HASH_PREFIX}' or '{FIRESTORE_SANITIZED_PREFIX}': {value}"
            raise InvalidKeyError(msg)


class FirestoreV1CollectionSanitizationStrategy(FirestoreV1KeySanitizationStrategy):
    """Sanitize Firestore collection IDs."""


class FirestoreStore(BaseContextManagerStore, BaseStore):
    """Firestore-based key-value store.

    This store uses Firebase DB as the key-value storage.
    The data is stored in collections.

    To avoid Firestore ID restrictions for keys and collections, use
    ``FirestoreV1KeySanitizationStrategy`` and
    ``FirestoreV1CollectionSanitizationStrategy``.
    """

    _client: firestore.AsyncClient

    @overload
    def __init__(
        self,
        client: firestore.AsyncClient,
        *,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Firestore store with a client.

        Args:
            client: The initialized Firestore client to use.
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

    @overload
    def __init__(
        self,
        *,
        credentials: Credentials | None = None,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Firestore store with credentials or Application Default Credentials.

        Args:
            credentials: Google credentials. If None, uses Application Default Credentials (ADC).
            project: Google project name. If None, inferred from credentials or environment.
            database: Database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """

    def __init__(
        self,
        client: firestore.AsyncClient | None = None,
        *,
        credentials: Credentials | None = None,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the Firestore store.

        Can be initialized with:
        - An existing AsyncClient
        - Explicit credentials
        - No credentials (uses Application Default Credentials)

        Args:
            client: The initialized Firestore client to use. If provided, other connection args are ignored.
            credentials: Google credentials. If None, uses Application Default Credentials (ADC).
            project: Google project name. If None, inferred from credentials or environment.
            database: Database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
            key_sanitization_strategy: The sanitization strategy to use for keys.
            collection_sanitization_strategy: The sanitization strategy to use for collections.
        """
        self._credentials = credentials
        self._project = project
        self._database = database
        serialization_adapter = BasicSerializationAdapter(value_format="string")

        if client is not None:
            self._client = client
            client_provided_by_user = True
        else:
            self._client = firestore.AsyncClient(credentials=self._credentials, project=self._project, database=self._database)
            client_provided_by_user = False
        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided_by_user,
            serialization_adapter=serialization_adapter,
            key_sanitization_strategy=key_sanitization_strategy,
            collection_sanitization_strategy=collection_sanitization_strategy,
        )

    @override
    async def _setup(self) -> None:
        """Register client cleanup if we own the client."""
        if not self._client_provided_by_user:
            self._exit_stack.callback(self._client.close)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str | None = None) -> ManagedEntry | None:
        """Get a managed entry from Firestore."""
        collection = collection or self.default_collection
        sanitized_collection, sanitized_key = self._sanitize_collection_and_key(collection=collection, key=key)
        response = await self._client.collection(sanitized_collection).document(sanitized_key).get()  # pyright: ignore[reportUnknownMemberType]
        doc = response.to_dict()
        if doc is None:
            return None
        return self._serialization_adapter.load_dict(data=doc)

    @override
    async def _get_managed_entries(self, *, collection: str, keys: Sequence[str]) -> list[ManagedEntry | None]:
        """Retrieve multiple managed entries from Firestore using batch get."""
        if not keys:
            return []

        sanitized_collection = self._sanitize_collection(collection=collection)
        sanitized_keys = [self._sanitize_key(key=key) for key in keys]

        # Get all documents in a single batch request
        doc_refs = [self._client.collection(sanitized_collection).document(key) for key in sanitized_keys]
        docs_by_id: dict[str, dict[str, object] | None] = {}

        async for doc in self._client.get_all(doc_refs):
            if doc.exists:
                docs_by_id[doc.id] = doc.to_dict()

        # Return results in the same order as keys
        result: list[ManagedEntry | None] = []
        for key in sanitized_keys:
            doc = docs_by_id.get(key)
            if doc is None:
                result.append(None)
            else:
                result.append(self._serialization_adapter.load_dict(data=doc))
        return result

    @override
    async def _put_managed_entry(self, *, key: str, managed_entry: ManagedEntry, collection: str | None = None) -> None:
        """Store a managed entry in Firestore."""
        collection = collection or self.default_collection
        sanitized_collection, sanitized_key = self._sanitize_collection_and_key(collection=collection, key=key)
        item = self._serialization_adapter.dump_dict(entry=managed_entry)
        await self._client.collection(sanitized_collection).document(sanitized_key).set(item)  # pyright: ignore[reportUnknownMemberType]

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
        """Store multiple managed entries in Firestore using batch write."""
        if not keys:
            return

        sanitized_collection = self._sanitize_collection(collection=collection)
        sanitized_keys = [self._sanitize_key(key=key) for key in keys]

        batch = self._client.batch()
        for key, managed_entry in zip(sanitized_keys, managed_entries, strict=True):
            doc_ref = self._client.collection(sanitized_collection).document(key)
            batch.set(doc_ref, self._serialization_adapter.dump_dict(entry=managed_entry))  # pyright: ignore[reportUnknownMemberType]
        await batch.commit()

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str | None = None) -> bool:
        """Delete a managed entry from Firestore.

        Returns True if the document existed and was deleted, False otherwise.
        """
        collection = collection or self.default_collection
        sanitized_collection, sanitized_key = self._sanitize_collection_and_key(collection=collection, key=key)
        # Check if document exists before deleting
        doc_ref = self._client.collection(sanitized_collection).document(sanitized_key)
        doc_snapshot = await doc_ref.get()  # pyright: ignore[reportUnknownMemberType]
        exists: bool = doc_snapshot.exists

        # Always perform the delete operation (idempotent)
        await doc_ref.delete()

        return bool(exists)

    @override
    async def _delete_managed_entries(self, *, keys: Sequence[str], collection: str) -> int:
        """Delete multiple managed entries from Firestore using batch delete."""
        if not keys:
            return 0

        sanitized_collection = self._sanitize_collection(collection=collection)
        sanitized_keys = [self._sanitize_key(key=key) for key in keys]

        # First check which documents exist (batch get)
        doc_refs = [self._client.collection(sanitized_collection).document(key) for key in sanitized_keys]
        existing_count = 0
        async for doc in self._client.get_all(doc_refs):
            if doc.exists:
                existing_count += 1

        # Then batch delete all requested keys (idempotent)
        batch = self._client.batch()
        for key in sanitized_keys:
            doc_ref = self._client.collection(sanitized_collection).document(key)
            batch.delete(doc_ref)
        await batch.commit()

        return existing_count
