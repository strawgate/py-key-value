from typing import Any, overload
from datetime import datetime, timezone
import time

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError as e:
    msg = "boto3 is required for DynamoDBStore. Install with: pip install boto3"
    raise ImportError(msg) from e

from typing_extensions import override

from kv_store_adapter.errors import StoreConnectionError
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.compound import compound_key, get_collections_from_compound_keys, get_keys_from_compound_keys
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry


class DynamoDBStore(BaseManagedKVStore):
    """DynamoDB-based key-value store with native TTL support."""

    _table_name: str
    _client: Any
    _dynamodb_resource: Any
    _table: Any

    @overload
    def __init__(self, *, table_name: str, client: Any) -> None: ...

    @overload
    def __init__(self, *, table_name: str, region_name: str = "us-east-1", aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None) -> None: ...

    def __init__(
        self,
        *,
        table_name: str,
        client: Any | None = None,
        region_name: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Initialize the DynamoDB store.

        Args:
            table_name: Name of the DynamoDB table to use.
            client: An existing boto3 DynamoDB client to use.
            region_name: AWS region name. Defaults to us-east-1.
            aws_access_key_id: AWS access key ID. Defaults to None (uses default AWS credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses default AWS credentials).
        """
        super().__init__()
        
        self._table_name = table_name
        
        if client:
            self._client = client
            self._dynamodb_resource = boto3.Session().resource('dynamodb')
        else:
            session_kwargs = {"region_name": region_name}
            if aws_access_key_id and aws_secret_access_key:
                session_kwargs.update({
                    "aws_access_key_id": aws_access_key_id,
                    "aws_secret_access_key": aws_secret_access_key,
                })
            
            session = boto3.Session(**session_kwargs)
            self._client = session.client('dynamodb')
            self._dynamodb_resource = session.resource('dynamodb')
        
        self._table = self._dynamodb_resource.Table(table_name)

    @override
    async def setup(self) -> None:
        """Initialize the DynamoDB table if it doesn't exist."""
        try:
            # Check if table exists
            response = self._client.describe_table(TableName=self._table_name)
            # Table exists, check if it has TTL enabled
            try:
                ttl_response = self._client.describe_time_to_live(TableName=self._table_name)
                ttl_status = ttl_response.get('TimeToLiveDescription', {}).get('TimeToLiveStatus')
                if ttl_status != 'ENABLED':
                    # Enable TTL on the expires_at_epoch attribute
                    self._client.update_time_to_live(
                        TableName=self._table_name,
                        TimeToLiveSpecification={
                            'AttributeName': 'expires_at_epoch',
                            'Enabled': True
                        }
                    )
            except ClientError:
                # TTL might not be supported or already configured
                pass
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                try:
                    self._client.create_table(
                        TableName=self._table_name,
                        KeySchema=[
                            {
                                'AttributeName': 'compound_key',
                                'KeyType': 'HASH'
                            }
                        ],
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'compound_key',
                                'AttributeType': 'S'
                            }
                        ],
                        BillingMode='PAY_PER_REQUEST'
                    )
                    
                    # Wait for table to be active
                    waiter = self._client.get_waiter('table_exists')
                    waiter.wait(TableName=self._table_name)
                    
                    # Enable TTL
                    try:
                        self._client.update_time_to_live(
                            TableName=self._table_name,
                            TimeToLiveSpecification={
                                'AttributeName': 'expires_at_epoch',
                                'Enabled': True
                            }
                        )
                    except ClientError:
                        # TTL might not be supported
                        pass
                        
                except ClientError as create_error:
                    raise StoreConnectionError(
                        message=f"Failed to create DynamoDB table: {create_error}"
                    ) from create_error
            else:
                raise StoreConnectionError(
                    message=f"Failed to access DynamoDB table: {e}"
                ) from e
        except NoCredentialsError as e:
            raise StoreConnectionError(
                message="AWS credentials not configured"
            ) from e

    @override
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection."""
        compound_key_str = compound_key(collection=collection, key=key)
        
        try:
            response = self._table.get_item(
                Key={'compound_key': compound_key_str}
            )
            
            if 'Item' not in response:
                return None
                
            item = response['Item']
            entry_json = item.get('entry_data')
            
            if not entry_json:
                return None
                
            return ManagedEntry.from_json(entry_json)
            
        except ClientError:
            return None

    @override
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None,
    ) -> None:
        """Store a managed entry by key in the specified collection."""
        compound_key_str = compound_key(collection=collection, key=key)
        
        item = {
            'compound_key': compound_key_str,
            'collection': collection,
            'key': key,
            'entry_data': cache_entry.to_json(),
        }
        
        # Add TTL if specified
        if ttl is not None and ttl > 0:
            expires_at_epoch = int(time.time() + ttl)
            item['expires_at_epoch'] = expires_at_epoch
        elif cache_entry.expires_at:
            # Use the entry's expiration time
            expires_at_epoch = int(cache_entry.expires_at.timestamp())
            item['expires_at_epoch'] = expires_at_epoch
            
        try:
            self._table.put_item(Item=item)
        except ClientError as e:
            # In a real implementation, you might want to handle specific errors
            # For now, we'll let it propagate
            raise

    @override
    async def delete(self, collection: str, key: str) -> bool:
        """Delete a key from the specified collection."""
        await self.setup_collection_once(collection=collection)
        
        compound_key_str = compound_key(collection=collection, key=key)
        
        try:
            response = self._table.delete_item(
                Key={'compound_key': compound_key_str},
                ReturnValues='ALL_OLD'
            )
            
            return 'Attributes' in response
            
        except ClientError:
            return False

    @override
    async def keys(self, collection: str) -> list[str]:
        """List all keys in the specified collection."""
        await self.setup_collection_once(collection=collection)
        
        try:
            from boto3.dynamodb.conditions import Attr
            
            response = self._table.scan(
                FilterExpression=Attr('collection').eq(collection),
                ProjectionExpression='#k',
                ExpressionAttributeNames={'#k': 'key'}
            )
            
            keys = []
            for item in response.get('Items', []):
                if 'key' in item:
                    keys.append(item['key'])
                    
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self._table.scan(
                    FilterExpression=Attr('collection').eq(collection),
                    ProjectionExpression='#k',
                    ExpressionAttributeNames={'#k': 'key'},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response.get('Items', []):
                    if 'key' in item:
                        keys.append(item['key'])
                        
            return keys
            
        except ClientError:
            return []

    @override
    async def clear_collection(self, collection: str) -> int:
        """Clear all keys in a collection, returning the number of keys deleted."""
        await self.setup_collection_once(collection=collection)
        
        try:
            from boto3.dynamodb.conditions import Attr
            
            # First, get all items in the collection
            response = self._table.scan(
                FilterExpression=Attr('collection').eq(collection),
                ProjectionExpression='compound_key'
            )
            
            deleted_count = 0
            
            # Delete items in batches
            items_to_delete = response.get('Items', [])
            deleted_count += len(items_to_delete)
            
            # Use batch_writer for efficient deletions
            with self._table.batch_writer() as batch:
                for item in items_to_delete:
                    batch.delete_item(Key={'compound_key': item['compound_key']})
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self._table.scan(
                    FilterExpression=Attr('collection').eq(collection),
                    ProjectionExpression='compound_key',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                items_to_delete = response.get('Items', [])
                deleted_count += len(items_to_delete)
                
                with self._table.batch_writer() as batch:
                    for item in items_to_delete:
                        batch.delete_item(Key={'compound_key': item['compound_key']})
                        
            return deleted_count
            
        except ClientError:
            return 0

    @override
    async def list_collections(self) -> list[str]:
        """List all available collection names."""
        await self.setup_once()
        
        try:
            response = self._table.scan(
                ProjectionExpression='#c',
                ExpressionAttributeNames={'#c': 'collection'}
            )
            
            collections = set()
            for item in response.get('Items', []):
                if 'collection' in item:
                    collections.add(item['collection'])
                    
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self._table.scan(
                    ProjectionExpression='#c',
                    ExpressionAttributeNames={'#c': 'collection'},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response.get('Items', []):
                    if 'collection' in item:
                        collections.add(item['collection'])
                        
            return list(collections)
            
        except ClientError:
            return []

    @override
    async def cull(self) -> None:
        """Remove expired entries. DynamoDB handles this automatically with TTL."""
        # DynamoDB automatically removes expired items based on the TTL attribute,
        # so we don't need to do anything here. Items are typically removed within 48 hours
        # of expiration.
        pass