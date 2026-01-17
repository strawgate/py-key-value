from typing import overload

from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
    BasicSerializationAdapter,
)

try:
    from google.cloud import firestore
    from google.oauth2.service_account import Credentials
except ImportError as e:
    msg = "FirestoreStore requires the `firestore` extra"
    raise ImportError(msg) from e


class FirestoreStore(BaseContextManagerStore, BaseStore):
    """Firestore-based key-value store.

    This store uses Firebase DB as the key-value storage.
    The data is stored in collections.
    """

    _client: firestore.AsyncClient | None

    @overload
    def __init__(self, client: firestore.AsyncClient, *, default_collection: str | None = None) -> None:
        """Initialize the Firestore store with a client. It defers project and database from client instance.

        Args:
            client: The initialized Firestore client to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self, *, credentials: Credentials, project: str | None = None, database: str | None = None, default_collection: str | None = None
    ) -> None:
        """Initialize the Firestore store with Google service account credentials.

        Args:
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        client: firestore.AsyncClient | None = None,
        *,
        credentials: Credentials | None = None,
        project: str | None = None,
        database: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the Firestore store with Google client or Google service account credentials.
        If provided with a client, uses it, otherwise connects using credentials.

        Args:
            client: The initialized Firestore client to use. Chosen by default if provided.
            credentials: Google service account credentials from google-cloud-auth module.
            project: Google project name.
            database: database name, defaults to '(default)' if not provided.
            default_collection: The default collection to use if no collection is provided.
        """
        self._credentials = credentials
        self._project = project
        self._database = database
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

    @property
    def _connected_client(self) -> firestore.AsyncClient:
        if not self._client:
            msg = "Client not connected"
            raise ValueError(msg)
        return self._client

    @override
    async def _get_managed_entry(self, *, key: str, collection: str | None = None) -> ManagedEntry | None:
        """Get a managed entry from Firestore."""
        collection = collection or self.default_collection
        response = await self._connected_client.collection(collection).document(key).get()  # pyright: ignore[reportUnknownMemberType]
        doc = response.to_dict()
        if doc is None:
            return None
        return self._serialization_adapter.load_dict(data=doc)

    @override
    async def _put_managed_entry(self, *, key: str, managed_entry: ManagedEntry, collection: str | None = None) -> None:
        """Store a managed entry in Firestore."""
        collection = collection or self.default_collection
        item = self._serialization_adapter.dump_dict(entry=managed_entry)
        await self._connected_client.collection(collection).document(key).set(item)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str | None = None) -> bool:
        """Delete a managed entry from Firestore."""
        collection = collection or self.default_collection
        await self._connected_client.collection(collection).document(key).delete()
        return True

    async def _close(self) -> None:
        """Close the Firestore client."""
        if self._client and not self._client_provided_by_user:
            self._client.close()
