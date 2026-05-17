"""Azure Table Storage async key-value store.

Backs the AsyncKeyValue protocol with Azure Table Storage. One Storage
account + one Table per store instance. Maps cleanly onto the
collection/key model:

    PartitionKey = collection
    RowKey       = key
    Value        = JSON-serialized ManagedEntry (string)
    ExpiresAt    = epoch seconds (set only for entries with TTL)

Azure Table Storage has no native TTL; this store handles expiry by
checking ExpiresAt on read (lazy expire).
"""

import contextlib
import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, overload

from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio._utils.sanitization import SanitizationStrategy
from key_value.aio.errors import InvalidKeyError
from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
)

try:
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
    from azure.data.tables import EdmType, EntityProperty, UpdateMode
    from azure.data.tables.aio import TableClient, TableServiceClient
except ImportError as e:
    msg = "AzureTablesStore requires py-key-value-aio[azure-tables]"
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from azure.core.credentials import AzureNamedKeyCredential, AzureSasCredential
    from azure.core.credentials_async import AsyncTokenCredential

    _AzureTablesCredential = AzureNamedKeyCredential | AzureSasCredential | AsyncTokenCredential
else:
    _AzureTablesCredential = Any


# ---------------------------------------------------------------------------
# Helper functions - module-level so they aren't part of the public surface.
# ---------------------------------------------------------------------------

# Azure Table Storage docs allow up to 1024 characters per PartitionKey/RowKey.
# We intentionally cap lower so sanitized values stay comfortably below service
# limits after URL encoding and quoting. Realistic keys (UUIDs, SHA-256 hashes,
# DCR client_ids, etc.) are well under this.
_AZURE_PK_RK_MAX_LEN = 256
_AZURE_PK_RK_FORBIDDEN_CHARS = frozenset("/\\#?")
# Azure Tables disallows control characters in both ASCII control ranges.
_C0_CONTROL_CHAR_BOUNDARY = 0x20
_C1_CONTROL_CHAR_START = 0x7F
_C1_CONTROL_CHAR_END = 0x9F


class AzureTablesSanitizationStrategy(SanitizationStrategy):
    """Sanitize values for Azure Tables PartitionKey and RowKey fields.

    Azure Table Storage rejects PartitionKey/RowKey values that exceed its
    URL/header limits or contain ``/``, ``\\``, ``#``, ``?``, C0 control
    characters, or C1 control characters. Values that violate those
    constraints are replaced with a deterministic ``H_``-prefixed SHA-256
    digest.

    The reserved ``H_``/``S_`` prefixes match the repo's other sanitization
    strategies and prevent collisions between caller-provided safe strings and
    generated sanitized keys.
    """

    def sanitize(self, value: str) -> str:
        """Return the original value if safe, otherwise a stable hashed key."""
        if self._is_safe(value):
            return value
        return "H_" + hashlib.sha256(value.encode("utf-8")).hexdigest()

    def validate(self, value: str) -> None:
        """Reject values that could collide with generated sanitized keys."""
        if value.startswith(("H_", "S_")):
            msg = f"Azure Tables keys cannot start with reserved prefixes 'H_' or 'S_': {value}"
            raise InvalidKeyError(msg)

    def try_unsanitize(self, value: str) -> str | None:
        """Return unchanged safe values; hashed values are not reversible."""
        if value.startswith(("H_", "S_")):
            return None
        return value

    def _is_safe(self, value: str) -> bool:
        if len(value) > _AZURE_PK_RK_MAX_LEN:
            return False
        return all(_is_safe_key_character(ch) for ch in value)


def _is_safe_key_character(value: str) -> bool:
    code_point = ord(value)
    return (
        value not in _AZURE_PK_RK_FORBIDDEN_CHARS
        and code_point >= _C0_CONTROL_CHAR_BOUNDARY
        and not (_C1_CONTROL_CHAR_START <= code_point <= _C1_CONTROL_CHAR_END)
    )


def _expires_at_from_entity(value: Any) -> datetime | None:
    """Convert a stored ExpiresAt property into a UTC datetime if it is valid."""
    raw_value = value.value if isinstance(value, EntityProperty) else value
    if type(raw_value) is int:
        return datetime.fromtimestamp(raw_value, tz=timezone.utc)
    return None


def _account_url_from_name(account_name: str) -> str:
    """Default Azure public-cloud Table endpoint for a storage account name."""
    return f"https://{account_name}.table.core.windows.net"


def _service_from_connection_string(connection_string: str) -> TableServiceClient:
    """Create a TableServiceClient from a connection string."""
    return TableServiceClient.from_connection_string(conn_str=connection_string)


def _service_from_endpoint_and_credential(*, endpoint: str, credential: _AzureTablesCredential) -> TableServiceClient:
    """Create a TableServiceClient from an explicit endpoint + Azure credential."""
    return TableServiceClient(endpoint=endpoint, credential=credential)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class AzureTablesStore(BaseContextManagerStore, BaseStore):
    """Azure Table Storage-backed async key-value store.

    Schema:
        PartitionKey -> collection
        RowKey       -> key
        Value        -> JSON-serialized ManagedEntry (string)
        ExpiresAt    -> Unix epoch seconds (omitted when no TTL)

    Azure Table Storage rejects PartitionKey/RowKey values that exceed its
    URL/header limits or contain ``/``, ``\\``, ``#``, ``?``, C0 control
    characters, or C1 control characters. Like other constrained stores in
    this package, sanitization is opt-in: pass
    ``AzureTablesSanitizationStrategy`` for collections and keys if callers may
    use out-of-spec values.

    Authentication patterns (mirrors DynamoDB's flexibility):

    1. Pre-constructed ``client: TableClient`` - caller manages lifecycle.
       Useful when the calling app already has its own auth/transport setup
       (Managed Identity via DefaultAzureCredential, custom retry policies,
       etc.). The store will not enter or exit the client's context.

    2. ``connection_string`` - simplest path for dev / shared-key scenarios.

    3. ``account_name`` + ``credential`` - recommended for production.
       ``credential`` may be ``AzureNamedKeyCredential``,
       ``AzureSasCredential``, or an ``AsyncTokenCredential`` (e.g.
       ``ManagedIdentityCredential``, ``WorkloadIdentityCredential``, or
       ``DefaultAzureCredential`` from ``azure-identity``). Account URL is
       derived as ``https://{account_name}.table.core.windows.net``.

    4. ``endpoint`` + ``credential`` - for Azurite (local emulator) or
       sovereign clouds where the endpoint isn't ``*.table.core.windows.net``.

    TTL: Azure Table Storage has no native TTL. Storage-side ExpiresAt is
    mirrored back onto the ManagedEntry on read so the base class's expiry
    logic applies as usual.
    """

    _service: TableServiceClient | None
    _table_client: TableClient | None
    _connection_string: str | None
    _endpoint: str | None
    _credential: _AzureTablesCredential | None
    _table_name: str
    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        client: TableClient,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from a pre-constructed TableClient.

        Args:
            client: A TableClient. The caller owns its lifecycle - the store
                will neither enter nor exit its async context.
            default_collection: Default collection name. Defaults to
                "default_collection".
            collection_sanitization_strategy: Strategy for Azure Tables
                PartitionKey values. Defaults to no sanitization.
            key_sanitization_strategy: Strategy for Azure Tables RowKey values.
                Defaults to no sanitization.
            auto_create: If True, attempt to create the table during setup.
                Existing tables are tolerated. If False, a missing table at
                setup time is reported as a ``StoreSetupError`` (wrapping a
                ``ValueError``).
        """

    @overload
    def __init__(
        self,
        *,
        connection_string: str,
        table_name: str,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from a connection string.

        Args:
            connection_string: Azure Storage connection string.
            table_name: Table name.
            default_collection: Default collection name.
            collection_sanitization_strategy: Strategy for Azure Tables
                PartitionKey values. Defaults to no sanitization.
            key_sanitization_strategy: Strategy for Azure Tables RowKey values.
                Defaults to no sanitization.
            auto_create: Whether to create the table if missing.
        """

    @overload
    def __init__(
        self,
        *,
        account_name: str,
        credential: _AzureTablesCredential,
        table_name: str,
        endpoint: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from an account name + Azure credential.

        Args:
            account_name: Storage account name (used to derive endpoint
                unless ``endpoint`` is explicitly passed).
            credential: An ``AzureNamedKeyCredential``,
                ``AzureSasCredential``, or ``AsyncTokenCredential``.
            table_name: Table name.
            endpoint: Optional explicit endpoint. Use for Azurite (e.g.
                ``http://127.0.0.1:10002/devstoreaccount1``) or sovereign
                clouds. Defaults to
                ``https://{account_name}.table.core.windows.net``.
            default_collection: Default collection name.
            collection_sanitization_strategy: Strategy for Azure Tables
                PartitionKey values. Defaults to no sanitization.
            key_sanitization_strategy: Strategy for Azure Tables RowKey values.
                Defaults to no sanitization.
            auto_create: Whether to create the table if missing.
        """

    @overload
    def __init__(
        self,
        *,
        endpoint: str,
        credential: _AzureTablesCredential,
        table_name: str,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from an explicit endpoint + Azure credential.

        Useful when the endpoint isn't ``{account}.table.core.windows.net``
        - Azurite, sovereign Azure clouds, custom DNS.
        """

    def __init__(
        self,
        *,
        client: TableClient | None = None,
        connection_string: str | None = None,
        account_name: str | None = None,
        credential: _AzureTablesCredential | None = None,
        endpoint: str | None = None,
        table_name: str | None = None,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
        auto_create: bool = True,
    ) -> None:
        """See the overloaded signatures above for argument documentation."""
        client_provided = client is not None

        if client is not None:
            # Caller-managed lifecycle. table_name comes from the client itself.
            self._table_client = client
            self._service = None
            self._connection_string = None
            self._endpoint = None
            self._credential = None
            self._table_name = client.table_name
        else:
            if not table_name:
                msg = "`table_name` is required when `client` is not provided"
                raise ValueError(msg)
            self._table_name = table_name
            self._table_client = None
            self._service = None
            self._connection_string = connection_string
            self._endpoint = endpoint or (_account_url_from_name(account_name) if account_name else None)
            self._credential = credential

        self._auto_create = auto_create

        super().__init__(
            default_collection=default_collection,
            collection_sanitization_strategy=collection_sanitization_strategy,
            key_sanitization_strategy=key_sanitization_strategy,
            client_provided_by_user=client_provided,
        )

    @property
    def _connected_table_client(self) -> TableClient:
        if not self._table_client:
            msg = "Table client is not connected. Use the store as an async context manager or call setup()."
            raise ValueError(msg)
        return self._table_client

    def _get_destination(self, *, collection: str, key: str) -> tuple[str, str]:
        """Return sanitized Azure Tables PartitionKey and RowKey values."""
        return self._sanitize_collection_and_key(collection=collection, key=key)

    @override
    async def _setup(self) -> None:
        """Setup the underlying clients and ensure the table exists.

        Behavior:
          * If we constructed our own service, enter its context and resolve
            a TableClient against the configured table.
          * If ``auto_create=True``, attempt to create the table; an existing
            table is tolerated.
          * If ``auto_create=False``, verify the table exists by attempting a
            single-page entity listing. Missing-table is surfaced as a
            ``ValueError`` (wrapped as ``StoreSetupError`` by the base class).
        """
        if not self._client_provided_by_user:
            service = self._service
            if service is None:
                if self._connection_string is not None:
                    service = _service_from_connection_string(self._connection_string)
                elif self._endpoint is not None and self._credential is not None:
                    service = _service_from_endpoint_and_credential(
                        endpoint=self._endpoint,
                        credential=self._credential,
                    )
                else:
                    msg = "AzureTablesStore requires connection_string= or endpoint/account_name + credential when client is not provided."
                    raise ValueError(msg)
                self._service = service
            await self._exit_stack.enter_async_context(service)
            self._table_client = service.get_table_client(table_name=self._table_name)

        if self._auto_create:
            with contextlib.suppress(ResourceExistsError):
                await self._connected_table_client.create_table()  # pyright: ignore[reportUnknownMemberType]
            return

        # auto_create=False: verify the table exists. list_entities triggers a
        # request on first iteration; ResourceNotFoundError means missing table.
        try:
            async for _ in self._connected_table_client.list_entities(  # pyright: ignore[reportUnknownMemberType]
                results_per_page=1,
            ):
                break
        except ResourceNotFoundError as e:
            msg = f"Table '{self._table_name}' does not exist. Either create the table manually or set auto_create=True."
            raise ValueError(msg) from e

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry from Azure Tables."""
        pk, rk = self._get_destination(collection=collection, key=key)
        try:
            entity: dict[str, Any] = await self._connected_table_client.get_entity(  # pyright: ignore[reportUnknownMemberType]
                partition_key=pk,
                row_key=rk,
            )
        except ResourceNotFoundError:
            return None

        json_value = entity.get("Value")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not isinstance(json_value, str) or not json_value:
            return None

        managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=json_value)

        # Storage-side ExpiresAt takes precedence over what's encoded in the
        # serialized ManagedEntry, mirroring DynamoDB's behavior. This matters
        # if a caller upserts the same key with a different TTL - the storage
        # property is the source of truth.
        expires_at_raw = entity.get("ExpiresAt")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if expires_at := _expires_at_from_entity(expires_at_raw):
            managed_entry.expires_at = expires_at

        return managed_entry

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry in Azure Tables (REPLACE semantics)."""
        json_value: str = self._serialization_adapter.dump_json(entry=managed_entry, key=key, collection=collection)

        pk, rk = self._get_destination(collection=collection, key=key)
        entity: dict[str, Any] = {
            "PartitionKey": pk,
            "RowKey": rk,
            "Value": json_value,
        }
        if managed_entry.expires_at is not None:
            entity["ExpiresAt"] = EntityProperty(
                int(managed_entry.expires_at.timestamp()),
                EdmType.INT64,
            )

        # REPLACE so put-after-put cleanly overwrites without merging stale
        # properties from a prior version of the entity.
        await self._connected_table_client.upsert_entity(  # pyright: ignore[reportUnknownMemberType]
            entity=entity, mode=UpdateMode.REPLACE
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry. Returns True iff an entity was actually deleted.

        The Azure SDK's ``TableClient.delete_entity`` silently succeeds when
        the entity is missing (per its documented "If the entity does not
        exist, this operation will succeed" behavior), so a naive
        ``except ResourceNotFoundError`` never fires. We GET first to detect
        existence, then DELETE if present. Costs one extra round-trip but
        gives the AsyncKeyValue contract semantics - True iff we actually
        removed something.
        """
        pk, rk = self._get_destination(collection=collection, key=key)
        try:
            await self._connected_table_client.get_entity(  # pyright: ignore[reportUnknownMemberType]
                partition_key=pk,
                row_key=rk,
                select=["PartitionKey"],  # tiny payload - we only care about existence
            )
        except ResourceNotFoundError:
            return False

        try:
            await self._connected_table_client.delete_entity(
                partition_key=pk,
                row_key=rk,
            )
        except ResourceNotFoundError:
            # Race - another caller deleted the entity between our GET and
            # DELETE. Treat as "we didn't actually delete it" since they did.
            return False
        return True
