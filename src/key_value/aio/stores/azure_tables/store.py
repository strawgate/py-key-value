"""Azure Table Storage async key-value store.

Backs the AsyncKeyValue protocol with Azure Table Storage. One Storage
account + one Table per store instance. Maps cleanly onto the
collection/key model:

    PartitionKey = collection
    RowKey       = key
    Value        = JSON-serialized ManagedEntry (string, ≤ 64 KB)
    ExpiresAt    = epoch seconds (set only for entries with TTL)

Azure Table Storage has no native TTL; this store handles expiry by
checking ExpiresAt on read (lazy expire) and exposing an explicit
``cull()`` for full sweeps.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, overload

from typing_extensions import override

from key_value.aio._utils.managed_entry import ManagedEntry
from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseCullStore,
)

try:
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
    from azure.data.tables import UpdateMode
    from azure.data.tables.aio import TableClient, TableServiceClient
except ImportError as e:
    msg = "AzureTablesStore requires py-key-value-aio[azure-tables]"
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from azure.core.credentials_async import AsyncTokenCredential


# ---------------------------------------------------------------------------
# Helper functions — module-level so they aren't part of the public surface.
# ---------------------------------------------------------------------------


def _account_url_from_name(account_name: str) -> str:
    """Default Azure public-cloud Table endpoint for a storage account name."""
    return f"https://{account_name}.table.core.windows.net"


def _service_from_connection_string(connection_string: str) -> TableServiceClient:
    """Create a TableServiceClient from a connection string."""
    return TableServiceClient.from_connection_string(conn_str=connection_string)


def _service_from_endpoint_and_credential(
    *, endpoint: str, credential: "AsyncTokenCredential"
) -> TableServiceClient:
    """Create a TableServiceClient from an explicit endpoint + AsyncTokenCredential."""
    return TableServiceClient(endpoint=endpoint, credential=credential)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class AzureTablesStore(BaseContextManagerStore, BaseCullStore):
    """Azure Table Storage-backed async key-value store.

    Schema:
        PartitionKey -> collection
        RowKey       -> key
        Value        -> JSON-serialized ManagedEntry (string)
        ExpiresAt    -> Unix epoch seconds (omitted when no TTL)

    Authentication patterns (mirrors DynamoDB's flexibility):

    1. Pre-constructed ``client: TableClient`` — caller manages lifecycle.
       Useful when the calling app already has its own auth/transport setup
       (Managed Identity via DefaultAzureCredential, custom retry policies,
       etc.). The store will not enter or exit the client's context.

    2. ``connection_string`` — simplest path for dev / shared-key scenarios.

    3. ``account_name`` + ``credential`` — recommended for production.
       ``credential`` should be an ``AsyncTokenCredential`` (e.g.
       ``ManagedIdentityCredential``, ``WorkloadIdentityCredential``,
       or ``DefaultAzureCredential`` from ``azure-identity``). Account URL
       is derived as ``https://{account_name}.table.core.windows.net``.

    4. ``endpoint`` + ``credential`` — for Azurite (local emulator) or
       sovereign clouds where the endpoint isn't ``*.table.core.windows.net``.

    TTL: Azure Table Storage has no native TTL. Two-pronged handling:
      * Lazy expire on read — storage-side ExpiresAt is mirrored back onto
        the ManagedEntry so the base class's expiry logic applies as usual.
      * Explicit ``cull()`` — implemented via BaseCullStore. Scans for
        entries with ``ExpiresAt < now`` and deletes them. Use on demand
        when the table accumulates stale entries; for low-write workloads
        like FastMCP OAuth state most callers won't need it.
    """

    _service: TableServiceClient | None
    _table_client: TableClient | None
    _table_name: str
    _auto_create: bool

    @overload
    def __init__(
        self,
        *,
        client: TableClient,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from a pre-constructed TableClient.

        Args:
            client: A TableClient. The caller owns its lifecycle — the store
                will neither enter nor exit its async context.
            default_collection: Default collection name. Defaults to
                "default_collection".
            auto_create: If True, attempt to create the table during setup.
                Existing tables are tolerated. Defaults to True.
        """

    @overload
    def __init__(
        self,
        *,
        connection_string: str,
        table_name: str,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from a connection string.

        Args:
            connection_string: Azure Storage connection string.
            table_name: Table name.
            default_collection: Default collection name.
            auto_create: Whether to create the table if missing.
        """

    @overload
    def __init__(
        self,
        *,
        account_name: str,
        credential: "AsyncTokenCredential",
        table_name: str,
        endpoint: str | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from an account name + AsyncTokenCredential.

        Args:
            account_name: Storage account name (used to derive endpoint
                unless ``endpoint`` is explicitly passed).
            credential: An ``AsyncTokenCredential`` (e.g.
                ``ManagedIdentityCredential``).
            table_name: Table name.
            endpoint: Optional explicit endpoint. Use for Azurite (e.g.
                ``http://127.0.0.1:10002/devstoreaccount1``) or sovereign
                clouds. Defaults to
                ``https://{account_name}.table.core.windows.net``.
            default_collection: Default collection name.
            auto_create: Whether to create the table if missing.
        """

    @overload
    def __init__(
        self,
        *,
        endpoint: str,
        credential: "AsyncTokenCredential",
        table_name: str,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize from an explicit endpoint + AsyncTokenCredential.

        Useful when the endpoint isn't ``{account}.table.core.windows.net``
        — Azurite, sovereign Azure clouds, custom DNS.
        """

    def __init__(
        self,
        *,
        client: TableClient | None = None,
        connection_string: str | None = None,
        account_name: str | None = None,
        credential: "AsyncTokenCredential | None" = None,
        endpoint: str | None = None,
        table_name: str | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """See the overloaded signatures above for argument documentation."""
        client_provided = client is not None

        # Validate that exactly one auth pattern was supplied.
        provided_patterns = sum(
            (
                client is not None,
                connection_string is not None,
                account_name is not None,
                endpoint is not None and credential is not None,
            )
        )
        if provided_patterns == 0:
            msg = (
                "AzureTablesStore requires one of: `client=`, "
                "`connection_string=`, `account_name=` + `credential=`, or "
                "`endpoint=` + `credential=`."
            )
            raise ValueError(msg)
        if provided_patterns > 1:
            msg = (
                "AzureTablesStore was given conflicting auth arguments. "
                "Pass exactly one of: `client`, `connection_string`, "
                "`account_name`+`credential`, `endpoint`+`credential`."
            )
            raise ValueError(msg)

        if client is not None:
            # Caller-managed lifecycle. table_name comes from the client itself.
            self._table_client = client
            self._service = None
            self._table_name = client.table_name
        else:
            if not table_name:
                msg = "`table_name` is required when `client` is not provided"
                raise ValueError(msg)
            self._table_name = table_name
            self._table_client = None

            if connection_string is not None:
                self._service = _service_from_connection_string(connection_string)
            else:
                # account_name + credential, or endpoint + credential.
                if credential is None:
                    msg = "`credential` is required with `account_name` or `endpoint`"
                    raise ValueError(msg)
                resolved_endpoint = endpoint or (
                    _account_url_from_name(account_name) if account_name else None
                )
                if resolved_endpoint is None:
                    msg = "Could not resolve a Table endpoint from the given arguments"
                    raise ValueError(msg)
                self._service = _service_from_endpoint_and_credential(
                    endpoint=resolved_endpoint, credential=credential
                )

        self._auto_create = auto_create

        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided,
        )

    @property
    def _connected_table_client(self) -> TableClient:
        if not self._table_client:
            msg = "Table client is not connected. Use the store as an async context manager or call setup()."
            raise ValueError(msg)
        return self._table_client

    @override
    async def _setup(self) -> None:
        """Setup the underlying clients and ensure the table exists."""
        if self._client_provided_by_user:
            # User-provided TableClient. Optionally create the table.
            if self._auto_create:
                try:
                    await self._connected_table_client.create_table()
                except ResourceExistsError:
                    pass
            return

        # We constructed our own TableServiceClient. Enter its async context
        # via the exit stack so cleanup happens on store close.
        service = self._service
        if service is None:
            # Should be unreachable given __init__ validation.
            msg = "AzureTablesStore: service client missing during setup"
            raise RuntimeError(msg)

        await self._exit_stack.enter_async_context(service)

        if self._auto_create:
            await service.create_table_if_not_exists(table_name=self._table_name)

        # The TableClient returned here shares transport with the service, so
        # we don't need to enter its context separately.
        self._table_client = service.get_table_client(table_name=self._table_name)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry from Azure Tables."""
        try:
            entity: dict[str, Any] = await self._connected_table_client.get_entity(  # pyright: ignore[reportUnknownMemberType, reportAssignmentType]
                partition_key=collection,
                row_key=key,
            )
        except ResourceNotFoundError:
            return None

        json_value = entity.get("Value")
        if not isinstance(json_value, str) or not json_value:
            return None

        managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=json_value)

        # Storage-side ExpiresAt takes precedence over what's encoded in the
        # serialized ManagedEntry, mirroring DynamoDB's behavior. This matters
        # if a caller upserts the same key with a different TTL — the storage
        # property is the source of truth.
        expires_at_raw = entity.get("ExpiresAt")
        if isinstance(expires_at_raw, int):
            managed_entry.expires_at = datetime.fromtimestamp(expires_at_raw, tz=timezone.utc)

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
        json_value: str = self._serialization_adapter.dump_json(
            entry=managed_entry, key=key, collection=collection
        )

        entity: dict[str, Any] = {
            "PartitionKey": collection,
            "RowKey": key,
            "Value": json_value,
        }
        if managed_entry.expires_at is not None:
            entity["ExpiresAt"] = int(managed_entry.expires_at.timestamp())

        # REPLACE so put-after-put cleanly overwrites without merging stale
        # properties from a prior version of the entity.
        await self._connected_table_client.upsert_entity(entity=entity, mode=UpdateMode.REPLACE)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry. Returns True iff an entity was actually deleted.

        The Azure SDK's ``TableClient.delete_entity`` silently succeeds when
        the entity is missing (per its documented "If the entity does not
        exist, this operation will succeed" behavior), so a naive
        ``except ResourceNotFoundError`` never fires. We GET first to detect
        existence, then DELETE if present. Costs one extra round-trip but
        gives the AsyncKeyValue contract semantics — True iff we actually
        removed something.
        """
        try:
            await self._connected_table_client.get_entity(  # pyright: ignore[reportUnknownMemberType]
                partition_key=collection,
                row_key=key,
                select=["PartitionKey"],  # tiny payload — we only care about existence
            )
        except ResourceNotFoundError:
            return False

        try:
            await self._connected_table_client.delete_entity(
                partition_key=collection,
                row_key=key,
            )
        except ResourceNotFoundError:
            # Race — another caller deleted the entity between our GET and
            # DELETE. Treat as "we didn't actually delete it" since they did.
            return False
        return True

    @override
    async def _cull(self) -> None:
        """Scan and delete entries whose ExpiresAt is in the past.

        Azure Table Storage has no native TTL, so this is a manual sweep.
        Use on demand when the table accumulates stale entries. Low-write
        workloads (e.g. OAuth state) typically don't need it.
        """
        now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
        query_filter = "ExpiresAt lt @now"
        parameters: dict[str, Any] = {"now": now_epoch}

        async for entity in self._connected_table_client.query_entities(  # pyright: ignore[reportUnknownMemberType]
            query_filter=query_filter,
            parameters=parameters,
        ):
            partition_key = entity.get("PartitionKey")
            row_key = entity.get("RowKey")
            if not (isinstance(partition_key, str) and isinstance(row_key, str)):
                continue
            try:
                await self._connected_table_client.delete_entity(
                    partition_key=partition_key,
                    row_key=row_key,
                )
            except ResourceNotFoundError:
                # Race — already deleted by lazy-expire-on-read or another
                # cull. Tolerable; cull's contract is best-effort cleanup.
                continue
