from collections.abc import Sequence
from types import TracebackType
from typing import Any, overload

from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import Self, override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
)

try:
    import aioboto3
    from aioboto3.session import Session  # noqa: TC002
    from types_aiobotocore_dynamodb.client import DynamoDBClient
except ImportError as e:
    msg = "DynamoDBStore requires py-key-value-aio[dynamodb]"
    raise ImportError(msg) from e


DEFAULT_PAGE_SIZE = 1000
PAGE_LIMIT = 1000

# DynamoDB batch operation limits
BATCH_GET_ITEM_LIMIT = 100
BATCH_WRITE_ITEM_LIMIT = 25


class DynamoDBStore(BaseContextManagerStore, BaseStore):
    """DynamoDB-based key-value store.

    This store uses a single DynamoDB table with a composite primary key:
    - collection (partition key)
    - key (sort key)
    """

    _session: aioboto3.Session  # pyright: ignore[reportAny]
    _table_name: str
    _endpoint_url: str | None
    _raw_client: Any  # DynamoDB client from aioboto3
    _client: DynamoDBClient | None

    @overload
    def __init__(self, *, client: DynamoDBClient, table_name: str, default_collection: str | None = None) -> None:
        """Initialize the DynamoDB store.

        Args:
            client: The DynamoDB client to use. You must have entered the context manager before passing this in.
            table_name: The name of the DynamoDB table to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        table_name: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the DynamoDB store.

        Args:
            table_name: The name of the DynamoDB table to use.
            region_name: AWS region name. Defaults to None (uses AWS default).
            endpoint_url: Custom endpoint URL (useful for local DynamoDB). Defaults to None.
            aws_access_key_id: AWS access key ID. Defaults to None (uses AWS default credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses AWS default credentials).
            aws_session_token: AWS session token. Defaults to None (uses AWS default credentials).
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        client: DynamoDBClient | None = None,
        table_name: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the DynamoDB store.

        Args:
            table_name: The name of the DynamoDB table to use.
            region_name: AWS region name. Defaults to None (uses AWS default).
            endpoint_url: Custom endpoint URL (useful for local DynamoDB). Defaults to None.
            aws_access_key_id: AWS access key ID. Defaults to None (uses AWS default credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses AWS default credentials).
            aws_session_token: AWS session token. Defaults to None (uses AWS default credentials).
            default_collection: The default collection to use if no collection is provided.
        """
        self._table_name = table_name
        if client:
            self._client = client
        else:
            session: Session = aioboto3.Session(  # pyright: ignore[reportAny]
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )

            self._raw_client = session.client(service_name="dynamodb", endpoint_url=endpoint_url)  # pyright: ignore[reportUnknownMemberType]

            self._client = None

        super().__init__(default_collection=default_collection)

    @override
    async def __aenter__(self) -> Self:
        if self._raw_client:
            self._client = await self._raw_client.__aenter__()
        await super().__aenter__()
        return self

    @override
    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await super().__aexit__(exc_type, exc_value, traceback)
        if self._client:
            await self._client.__aexit__(exc_type, exc_value, traceback)

    @property
    def _connected_client(self) -> DynamoDBClient:
        if not self._client:
            msg = "Client not connected"
            raise ValueError(msg)
        return self._client

    @override
    async def _setup(self) -> None:
        """Setup the DynamoDB client and ensure table exists."""

        if not self._client:
            self._client = await self._raw_client.__aenter__()

        try:
            await self._connected_client.describe_table(TableName=self._table_name)  # pyright: ignore[reportUnknownMemberType]
        except self._connected_client.exceptions.ResourceNotFoundException:  # pyright: ignore[reportUnknownMemberType]
            # Create the table with composite primary key
            await self._connected_client.create_table(  # pyright: ignore[reportUnknownMemberType]
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "collection", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "key", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "collection", "AttributeType": "S"},
                    {"AttributeName": "key", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",  # On-demand billing
            )

            # Wait for table to be active
            waiter = self._connected_client.get_waiter("table_exists")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            await waiter.wait(TableName=self._table_name)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry from DynamoDB."""
        response = await self._connected_client.get_item(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            TableName=self._table_name,
            Key={
                "collection": {"S": collection},
                "key": {"S": key},
            },
        )

        item = response.get("Item")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not item:
            return None

        json_value = item.get("value", {}).get("S")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not json_value:
            return None

        return ManagedEntry.from_json(json_str=json_value)  # pyright: ignore[reportUnknownArgumentType]

    @override
    async def _get_managed_entries(self, *, collection: str, keys: list[str]) -> list[ManagedEntry | None]:
        """Retrieve multiple managed entries from DynamoDB using batch_get_item."""
        if not keys:
            return []

        entries: dict[str, ManagedEntry | None] = dict.fromkeys(keys)

        # Process in batches of BATCH_GET_ITEM_LIMIT
        for i in range(0, len(keys), BATCH_GET_ITEM_LIMIT):
            batch_keys = keys[i : i + BATCH_GET_ITEM_LIMIT]

            request_items = {
                self._table_name: {
                    "Keys": [{"collection": {"S": collection}, "key": {"S": key}} for key in batch_keys],
                }
            }

            response = await self._connected_client.batch_get_item(RequestItems=request_items)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            items = response.get("Responses", {}).get(self._table_name, [])  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            for item in items:  # pyright: ignore[reportUnknownVariableType]
                item_key = item.get("key", {}).get("S")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                json_value = item.get("value", {}).get("S")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                if item_key and json_value:
                    entries[item_key] = ManagedEntry.from_json(json_str=json_value)  # pyright: ignore[reportUnknownArgumentType]

        return [entries[key] for key in keys]

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry in DynamoDB."""
        json_value = managed_entry.to_json()

        item: dict[str, Any] = {
            "collection": {"S": collection},
            "key": {"S": key},
            "value": {"S": json_value},
        }

        # Add TTL if present
        if managed_entry.ttl is not None and managed_entry.created_at:
            # DynamoDB TTL expects a Unix timestamp
            ttl_timestamp = int(managed_entry.created_at.timestamp() + managed_entry.ttl)
            item["ttl"] = {"N": str(ttl_timestamp)}

        await self._connected_client.put_item(  # pyright: ignore[reportUnknownMemberType]
            TableName=self._table_name,
            Item=item,
        )

    @override
    async def _put_managed_entries(self, *, collection: str, keys: list[str], managed_entries: Sequence[ManagedEntry]) -> None:
        """Store multiple managed entries in DynamoDB using batch_write_item."""
        if not keys:
            return

        # Process in batches of BATCH_WRITE_ITEM_LIMIT
        for i in range(0, len(keys), BATCH_WRITE_ITEM_LIMIT):
            batch_keys = keys[i : i + BATCH_WRITE_ITEM_LIMIT]
            batch_entries = managed_entries[i : i + BATCH_WRITE_ITEM_LIMIT]

            put_requests = []
            for key, managed_entry in zip(batch_keys, batch_entries, strict=True):
                json_value = managed_entry.to_json()

                item: dict[str, Any] = {
                    "collection": {"S": collection},
                    "key": {"S": key},
                    "value": {"S": json_value},
                }

                # Add TTL if present
                if managed_entry.ttl is not None and managed_entry.created_at:
                    ttl_timestamp = int(managed_entry.created_at.timestamp() + managed_entry.ttl)
                    item["ttl"] = {"N": str(ttl_timestamp)}

                put_requests.append({"PutRequest": {"Item": item}})

            request_items = {self._table_name: put_requests}

            await self._connected_client.batch_write_item(RequestItems=request_items)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from DynamoDB."""
        response = await self._connected_client.delete_item(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            TableName=self._table_name,
            Key={
                "collection": {"S": collection},
                "key": {"S": key},
            },
            ReturnValues="ALL_OLD",
        )

        # Return True if an item was actually deleted
        return "Attributes" in response  # pyright: ignore[reportUnknownArgumentType]

    @override
    async def _delete_managed_entries(self, *, keys: list[str], collection: str) -> int:
        """Delete multiple managed entries from DynamoDB using batch_write_item."""
        if not keys:
            return 0

        deleted_count = 0

        # Process in batches of BATCH_WRITE_ITEM_LIMIT
        for i in range(0, len(keys), BATCH_WRITE_ITEM_LIMIT):
            batch_keys = keys[i : i + BATCH_WRITE_ITEM_LIMIT]

            delete_requests = [
                {
                    "DeleteRequest": {
                        "Key": {
                            "collection": {"S": collection},
                            "key": {"S": key},
                        }
                    }
                }
                for key in batch_keys
            ]

            request_items = {self._table_name: delete_requests}

            # Note: batch_write_item doesn't return information about which items were deleted
            # We assume all items in the batch were deleted (even if they didn't exist)
            await self._connected_client.batch_write_item(RequestItems=request_items)  # pyright: ignore[reportUnknownMemberType]
            deleted_count += len(batch_keys)

        return deleted_count

    @override
    async def _close(self) -> None:
        """Close the DynamoDB client."""
        if self._client:
            await self._client.__aexit__(None, None, None)  # pyright: ignore[reportUnknownMemberType]
