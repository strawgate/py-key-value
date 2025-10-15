from typing import Any

from key_value.shared.type_checking.bear_spray import bear_spray
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseDestroyCollectionStore,
    BaseEnumerateKeysStore,
    BaseStore,
)

try:
    import aioboto3
except ImportError as e:
    msg = "DynamoDBStore requires py-key-value-aio[dynamodb]"
    raise ImportError(msg) from e


DEFAULT_PAGE_SIZE = 1000
PAGE_LIMIT = 1000


class DynamoDBStore(BaseEnumerateKeysStore, BaseDestroyCollectionStore, BaseContextManagerStore, BaseStore):
    """DynamoDB-based key-value store.

    This store uses a single DynamoDB table with a composite primary key:
    - collection (partition key)
    - key (sort key)

    Each item stores the managed entry as a JSON string in the 'value' attribute.
    """

    _session: aioboto3.Session  # pyright: ignore[reportAny]
    _table_name: str
    _endpoint_url: str | None
    _client: Any  # DynamoDB client from aioboto3

    @bear_spray
    def __init__(
        self,
        *,
        table_name: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the DynamoDB store.

        Args:
            table_name: The name of the DynamoDB table to use.
            region_name: AWS region name. Defaults to None (uses AWS default).
            endpoint_url: Custom endpoint URL (useful for local DynamoDB). Defaults to None.
            aws_access_key_id: AWS access key ID. Defaults to None (uses AWS default credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses AWS default credentials).
            default_collection: The default collection to use if no collection is provided.
        """
        self._table_name = table_name
        self._session = aioboto3.Session(  # pyright: ignore[reportAny]
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._endpoint_url = endpoint_url
        self._client = None

        super().__init__(default_collection=default_collection)

    @override
    async def _setup(self) -> None:
        """Setup the DynamoDB client and ensure table exists."""
        # Create a persistent client
        self._client = await self._session.client("dynamodb", endpoint_url=self._endpoint_url).__aenter__()  # pyright: ignore[reportUnknownMemberType, reportAny, reportGeneralTypeIssues, reportUnknownVariableType]

        # Check if table exists, if not create it
        try:
            await self._client.describe_table(TableName=self._table_name)  # pyright: ignore[reportUnknownMemberType]
        except self._client.exceptions.ResourceNotFoundException:  # pyright: ignore[reportUnknownMemberType]
            # Create the table with composite primary key
            await self._client.create_table(  # pyright: ignore[reportUnknownMemberType]
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
            waiter = self._client.get_waiter("table_exists")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            await waiter.wait(TableName=self._table_name)  # pyright: ignore[reportUnknownMemberType]

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry from DynamoDB."""
        response = await self._client.get_item(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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

        await self._client.put_item(  # pyright: ignore[reportUnknownMemberType]
            TableName=self._table_name,
            Item=item,
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from DynamoDB."""
        response = await self._client.delete_item(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
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
    async def _get_collection_keys(self, *, collection: str, limit: int | None = None) -> list[str]:
        """List all keys in a collection."""
        limit = min(limit or DEFAULT_PAGE_SIZE, PAGE_LIMIT)

        response: Any = await self._client.query(  # pyright: ignore[reportUnknownMemberType]
            TableName=self._table_name,
            KeyConditionExpression="collection = :collection",
            ExpressionAttributeValues={
                ":collection": {"S": collection},
            },
            ProjectionExpression="key",
            Limit=limit,
        )

        items = response.get("Items", [])
        return [item["key"]["S"] for item in items if "key" in item]

    @override
    async def _delete_collection(self, *, collection: str) -> bool:
        """Delete all items in a collection."""
        # DynamoDB doesn't have a native "delete collection" operation
        # We need to query all keys and delete them individually
        # Query all items in the collection
        response: Any = await self._client.query(  # pyright: ignore[reportUnknownMemberType]
            TableName=self._table_name,
            KeyConditionExpression="collection = :collection",
            ExpressionAttributeValues={
                ":collection": {"S": collection},
            },
            ProjectionExpression="collection, key",
        )

        items = response.get("Items", [])

        # Delete each item
        for item in items:
            await self._client.delete_item(  # pyright: ignore[reportUnknownMemberType]
                TableName=self._table_name,
                Key={
                    "collection": {"S": item["collection"]["S"]},
                    "key": {"S": item["key"]["S"]},
                },
            )

        return len(items) > 0  # pyright: ignore[reportUnknownArgumentType]

    @override
    async def _close(self) -> None:
        """Close the DynamoDB client."""
        if self._client:
            await self._client.__aexit__(None, None, None)  # pyright: ignore[reportUnknownMemberType]
            self._client = None
