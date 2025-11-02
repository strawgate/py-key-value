from types import TracebackType
from typing import TYPE_CHECKING, Any, overload

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitize import hash_excess_length
from typing_extensions import Self, override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
)

# HTTP status code for not found errors
HTTP_NOT_FOUND = 404

# S3 key length limit is 1024 bytes
# We allocate space for collection, separator, and key
# Using 500 bytes for each allows for the separator and stays well under 1024
MAX_COLLECTION_LENGTH = 500
MAX_KEY_LENGTH = 500

try:
    import aioboto3
    from aioboto3.session import Session  # noqa: TC002
except ImportError as e:
    msg = "S3Store requires py-key-value-aio[s3]"
    raise ImportError(msg) from e

# aioboto3 generates types at runtime, so we use AioBaseClient at runtime but S3Client during static type checking
if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client
else:
    from aiobotocore.client import AioBaseClient as S3Client


class S3Store(BaseContextManagerStore, BaseStore):
    """AWS S3-based key-value store.

    This store uses AWS S3 to store key-value pairs as objects. Each entry is stored
    as a separate S3 object with the path format: {collection}/{key}. The ManagedEntry
    is serialized to JSON and stored as the object body. TTL information is stored in
    S3 object metadata and checked client-side during retrieval (S3 lifecycle policies
    can be configured separately for background cleanup, but don't provide atomic TTL+retrieval).

    Example:
        Basic usage with automatic AWS credentials:

        >>> async with S3Store(bucket_name="my-kv-store") as store:
        ...     await store.put(key="user:123", value={"name": "Alice"}, ttl=3600)
        ...     user = await store.get(key="user:123")

        With custom AWS credentials:

        >>> async with S3Store(
        ...     bucket_name="my-kv-store",
        ...     region_name="us-west-2",
        ...     aws_access_key_id="...",
        ...     aws_secret_access_key="...",
        ... ) as store:
        ...     await store.put(key="config", value={"setting": "value"})

        For local testing with LocalStack:

        >>> async with S3Store(
        ...     bucket_name="test-bucket",
        ...     endpoint_url="http://localhost:4566",
        ... ) as store:
        ...     await store.put(key="test", value={"data": "test"})
    """

    _bucket_name: str
    _endpoint_url: str | None
    _raw_client: Any  # S3 client from aioboto3
    _client: S3Client | None

    @overload
    def __init__(self, *, client: S3Client, bucket_name: str, default_collection: str | None = None) -> None:
        """Initialize the S3 store with a pre-configured client.

        Args:
            client: The S3 client to use. You must have entered the context manager before passing this in.
            bucket_name: The name of the S3 bucket to use.
            default_collection: The default collection to use if no collection is provided.
        """

    @overload
    def __init__(
        self,
        *,
        bucket_name: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the S3 store with AWS credentials.

        Args:
            bucket_name: The name of the S3 bucket to use.
            region_name: AWS region name. Defaults to None (uses AWS default).
            endpoint_url: Custom endpoint URL (useful for LocalStack/MinIO). Defaults to None.
            aws_access_key_id: AWS access key ID. Defaults to None (uses AWS default credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses AWS default credentials).
            aws_session_token: AWS session token. Defaults to None (uses AWS default credentials).
            default_collection: The default collection to use if no collection is provided.
        """

    def __init__(
        self,
        *,
        client: S3Client | None = None,
        bucket_name: str,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        default_collection: str | None = None,
    ) -> None:
        """Initialize the S3 store.

        Args:
            client: The S3 client to use. Defaults to None (creates a new client).
            bucket_name: The name of the S3 bucket to use.
            region_name: AWS region name. Defaults to None (uses AWS default).
            endpoint_url: Custom endpoint URL (useful for LocalStack/MinIO). Defaults to None.
            aws_access_key_id: AWS access key ID. Defaults to None (uses AWS default credentials).
            aws_secret_access_key: AWS secret access key. Defaults to None (uses AWS default credentials).
            aws_session_token: AWS session token. Defaults to None (uses AWS default credentials).
            default_collection: The default collection to use if no collection is provided.
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url

        if client:
            self._client = client
            self._raw_client = None
        else:
            session: Session = aioboto3.Session(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )

            self._raw_client = session.client(service_name="s3", endpoint_url=endpoint_url)  # pyright: ignore[reportUnknownMemberType]
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
        if self._client and self._raw_client:
            await self._client.__aexit__(exc_type, exc_value, traceback)
            self._client = None

    @property
    def _connected_client(self) -> S3Client:
        """Get the connected S3 client.

        Raises:
            ValueError: If the client is not connected.

        Returns:
            The connected S3 client.
        """
        if not self._client:
            msg = "Client not connected"
            raise ValueError(msg)
        return self._client

    @override
    async def _setup(self) -> None:
        """Setup the S3 client and ensure bucket exists.

        This method creates the S3 bucket if it doesn't already exist. It uses the
        HeadBucket operation to check for bucket existence and creates it if not found.
        """
        if not self._client and self._raw_client:
            self._client = await self._raw_client.__aenter__()

        from botocore.exceptions import ClientError

        try:
            # Check if bucket exists
            await self._connected_client.head_bucket(Bucket=self._bucket_name)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            # Only proceed with bucket creation if it's a 404/NoSuchBucket error
            error_code = e.response.get("Error", {}).get("Code", "")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if error_code in ("404", "NoSuchBucket") or http_status == HTTP_NOT_FOUND:
                # Bucket doesn't exist, create it
                import contextlib

                with contextlib.suppress(self._connected_client.exceptions.BucketAlreadyOwnedByYou):  # pyright: ignore[reportUnknownMemberType]
                    # Build create_bucket parameters
                    create_params: dict[str, Any] = {"Bucket": self._bucket_name}

                    # Get region from client metadata
                    region_name = getattr(self._connected_client.meta, "region_name", None)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                    # For regions other than us-east-1, we need to specify LocationConstraint
                    # Skip this for custom endpoints (LocalStack, MinIO) which may not support it
                    if region_name and region_name != "us-east-1" and not self._endpoint_url:
                        create_params["CreateBucketConfiguration"] = {"LocationConstraint": region_name}

                    await self._connected_client.create_bucket(**create_params)  # pyright: ignore[reportUnknownMemberType]
            else:
                # Re-raise authentication, permission, or other errors
                raise

    def _get_s3_key(self, *, collection: str, key: str) -> str:
        """Generate the S3 object key for a given collection and key.

        S3 has a maximum key length of 1024 bytes. To ensure compliance, we hash
        long collection or key names to stay within limits while maintaining uniqueness.

        Args:
            collection: The collection name.
            key: The key within the collection.

        Returns:
            The S3 object key in format: {collection}/{key}
        """
        # Hash collection and key if they exceed their max byte lengths
        # This ensures the combined S3 key stays under 1024 bytes
        safe_collection = hash_excess_length(collection, MAX_COLLECTION_LENGTH, length_is_bytes=True)
        safe_key = hash_excess_length(key, MAX_KEY_LENGTH, length_is_bytes=True)
        return f"{safe_collection}/{safe_key}"

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        """Retrieve a managed entry from S3.

        This method fetches the object from S3, deserializes the JSON body to a ManagedEntry,
        and checks for client-side TTL expiration. If the entry has expired, it is deleted
        and None is returned.

        Args:
            key: The key to retrieve.
            collection: The collection to retrieve from.

        Returns:
            The ManagedEntry if found and not expired, otherwise None.
        """
        s3_key = self._get_s3_key(collection=collection, key=key)

        try:
            response = await self._connected_client.get_object(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                Bucket=self._bucket_name,
                Key=s3_key,
            )

            # Read the object body and ensure the streaming body is closed
            async with response["Body"] as stream:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                body_bytes = await stream.read()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            json_value = body_bytes.decode("utf-8")  # pyright: ignore[reportUnknownMemberType]

            # Deserialize to ManagedEntry
            managed_entry = self._serialization_adapter.load_json(json_str=json_value)

            # Check for client-side expiration
            if managed_entry.is_expired:
                # Entry expired, return None without deleting
                return None
            return managed_entry  # noqa: TRY300

        except self._connected_client.exceptions.NoSuchKey:  # pyright: ignore[reportUnknownMemberType]
            # Object doesn't exist
            return None

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry in S3.

        This method serializes the ManagedEntry to JSON and stores it as an S3 object.
        TTL information is stored in the object metadata for potential use by S3 lifecycle
        policies (though lifecycle policies don't support atomic TTL+retrieval, so client-side
        checking is still required).

        Args:
            key: The key to store.
            collection: The collection to store in.
            managed_entry: The ManagedEntry to store.
        """
        s3_key = self._get_s3_key(collection=collection, key=key)
        json_value = self._serialization_adapter.dump_json(entry=managed_entry)

        # Prepare metadata
        metadata: dict[str, str] = {}
        if managed_entry.expires_at:
            metadata["expires-at"] = managed_entry.expires_at.isoformat()
        if managed_entry.created_at:
            metadata["created-at"] = managed_entry.created_at.isoformat()

        await self._connected_client.put_object(  # pyright: ignore[reportUnknownMemberType]
            Bucket=self._bucket_name,
            Key=s3_key,
            Body=json_value.encode("utf-8"),
            ContentType="application/json",
            Metadata=metadata,
        )

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry from S3.

        Args:
            key: The key to delete.
            collection: The collection to delete from.

        Returns:
            True if an object was deleted, False if the object didn't exist.
        """
        s3_key = self._get_s3_key(collection=collection, key=key)

        from botocore.exceptions import ClientError

        try:
            # Check if object exists before deletion
            await self._connected_client.head_object(  # pyright: ignore[reportUnknownMemberType]
                Bucket=self._bucket_name,
                Key=s3_key,
            )

        except ClientError as e:
            # Check if it's a 404/not found error
            error_code = e.response.get("Error", {}).get("Code", "")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if error_code in ("404", "NoSuchKey") or http_status == HTTP_NOT_FOUND:
                # Object doesn't exist
                return False
            # Re-raise other errors (auth, network, etc.)
            raise
        else:
            # Object exists, delete it
            await self._connected_client.delete_object(  # pyright: ignore[reportUnknownMemberType]
                Bucket=self._bucket_name,
                Key=s3_key,
            )
            return True

    @override
    async def _close(self) -> None:
        """Close the S3 client."""
        if self._client and self._raw_client:
            await self._client.__aexit__(None, None, None)  # pyright: ignore[reportUnknownMemberType]
            self._client = None
