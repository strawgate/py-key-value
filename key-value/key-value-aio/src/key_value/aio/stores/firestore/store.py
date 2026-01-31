from typing import overload

from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

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


class FirestoreStore(BaseContextManagerStore, BaseStore):
    """Firestore-based key-value store.

    This store uses Firebase DB as the key-value storage.
    The data is stored in collections.
    """

    _client: firestore.AsyncClient
    _auto_create: bool

    @overload
    def __init__(self, client: firestore.AsyncClient, *, default_collection: str | None = None, auto_create: bool = True) -> None:
        """Initialize the Firestore store with a client. It defers project and database from client instance.

        Args:
            client: The initialized Firestore client to use.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create collections if they don't exist. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        credentials: Credentials,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Firestore store with Google service account credentials.

        Args:
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create collections if they don't exist. Defaults to True.
        """

    def __init__(
        self,
        client: firestore.AsyncClient | None = None,
        *,
        credentials: Credentials | None = None,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the Firestore store with Google client or Google service account credentials.
        If provided with a client, uses it, otherwise connects using credentials.

        Args:
            client: The initialized Firestore client to use. Chosen by default if provided.
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create collections if they don't exist. Defaults to True.
                When False, raises ValueError if a collection doesn't exist.
        """
        self._credentials = credentials
        self._project = project
        self._database = database
        self._auto_create = auto_create
        serialization_adapter = BasicSerializationAdapter(value_format="string")

        if client:
            self._client = client
            client_provided_by_user = True
        else:
            self._client = firestore.AsyncClient(credentials=self._credentials, project=self._project, database=self._database)
            client_provided_by_user = False
        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided_by_user,
            serialization_adapter=serialization_adapter,
        )

    @override
    async def _setup(self) -> None:
        """Register client cleanup if we own the client."""
        if not self._client_provided_by_user:
            self._exit_stack.callback(self._client.close)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        """Set up a Firestore collection.

        Firestore collections are created automatically on first write, so this method
        only validates that the collection exists when auto_create=False.
        """
        if not self._auto_create:
            # Check if collection exists by trying to list documents
            # Firestore creates collections implicitly, so we check if any documents exist
            col_ref = self._client.collection(collection)  # pyright: ignore[reportUnknownMemberType,reportOptionalMemberAccess]
            # Get just one document to see if collection has any data
            docs = col_ref.limit(1).stream()  # pyright: ignore[reportUnknownMemberType]

            # Check if any documents exist
            has_documents = False
            async for _ in docs:  # pyright: ignore[reportUnknownMemberType]
                has_documents = True
                break

            if not has_documents:
                msg = (
                    f"Collection '{collection}' does not exist or is empty. Either create the collection manually or set auto_create=True."
                )
                raise ValueError(msg)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str | None = None) -> ManagedEntry | None:
        """Get a managed entry from Firestore."""
        collection = collection or self.default_collection
        response = await self._client.collection(collection).document(key).get()  # pyright: ignore[reportUnknownMemberType,reportOptionalMemberAccess]
        doc = response.to_dict()  # pyright: ignore[reportUnknownMemberType]
        if doc is None:
            return None
        return self._serialization_adapter.load_dict(data=doc)

    @override
    async def _put_managed_entry(self, *, key: str, managed_entry: ManagedEntry, collection: str | None = None) -> None:
        """Store a managed entry in Firestore."""
        collection = collection or self.default_collection
        item = self._serialization_adapter.dump_dict(entry=managed_entry)
        await self._client.collection(collection).document(key).set(item)  # pyright: ignore[reportUnknownMemberType,reportOptionalMemberAccess]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str | None = None) -> bool:
        """Delete a managed entry from Firestore.

        Returns True if the document existed and was deleted, False otherwise.
        """
        collection = collection or self.default_collection
        # Check if document exists before deleting
        doc_ref = self._client.collection(collection).document(key)  # pyright: ignore[reportUnknownMemberType,reportOptionalMemberAccess]
        doc_snapshot = await doc_ref.get()  # pyright: ignore[reportUnknownMemberType]
        exists: bool = doc_snapshot.exists  # pyright: ignore[reportUnknownMemberType]

        # Always perform the delete operation (idempotent)
        await doc_ref.delete()  # pyright: ignore[reportUnknownMemberType]

        return bool(exists)
