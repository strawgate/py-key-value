from types import TracebackType
from typing import TYPE_CHECKING, Any, overload

from key_value.shared.utils.managed_entry import ManagedEntry
from key_value.shared.utils.sanitization import SanitizationStrategy
from key_value.shared.utils.sanitize import hash_excess_length
from typing_extensions import Self, override

from key_value.aio.stores.base import (
    BaseContextManagerStore,
    BaseStore,
)

HTTP_NOT_FOUND = 404

# S3 key length limit is 1024 bytes
# Allocating 500 bytes each for collection and key stays well under the limit
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


class S3KeySanitizationStrategy(SanitizationStrategy):
    """Sanitization strategy for S3 keys with byte-aware length limits.

    S3 has a maximum key length of 1024 bytes (UTF-8 encoded). This strategy
    hashes keys that exceed the specified byte limit to ensure compliance.

    Args:
        max_bytes: Maximum key length in bytes. Defaults to 500.
    """

    def __init__(self, max_bytes: int = MAX_KEY_LENGTH) -> None:
        """Initialize the S3 key sanitization strategy.

        Args:
            max_bytes: Maximum key length in bytes.
        """
        self.max_bytes = max_bytes

    def sanitize(self, value: str) -> str:
        """Hash the value if it exceeds max_bytes when UTF-8 encoded.

        Args:
            value: The key to sanitize.

        Returns:
            The original value if within limit, or truncated+hashed if too long.
        """
        return hash_excess_length(value, self.max_bytes, length_is_bytes=True)

    def validate(self, value: str) -> None:
        """No validation needed for S3 keys."""


class S3CollectionSanitizationStrategy(S3KeySanitizationStrategy):
    """Sanitization strategy for S3 collection names with byte-aware length limits.

    This is identical to S3KeySanitizationStrategy but uses a default of 500 bytes
    for collection names to match the S3 key format {collection}/{key}.
    """

    def __init__(self, max_bytes: int = MAX_COLLECTION_LENGTH) -> None:
        """Initialize the S3 collection sanitization strategy.

        Args:
            max_bytes: Maximum collection name length in bytes.
        """
        super().__init__(max_bytes=max_bytes)


class S3Store(BaseContextManagerStore, BaseStore):
    """AWS S3-based key-value store.

    This store uses AWS S3 to store key-value pairs as objects. Each entry is stored
    as a separate S3 object with the path format: {collection}/{key}. The ManagedEntry
    is serialized to JSON and stored as the object body. TTL information is stored in
    S3 object metadata and checked client-side during retrieval (S3 lifecycle policies
    can be configured separately for background cleanup, but don't provide atomic TTL+retrieval).

    By default, collections and keys are not sanitized. This means you must ensure that
    the combined "{collection}/{key}" path does not exceed S3's 1024-byte limit when UTF-8 encoded.

    To handle long collection or key names, use the S3CollectionSanitizationStrategy and
    S3KeySanitizationStrategy which will hash values exceeding the byte limit.

    Example:
        Basic usage with automatic AWS credentials:

        >>> async with S3Store(bucket_name="my-kv-store") as store:
        ...     await store.put(key="user:123", value={"name": "Alice"}, ttl=3600)
        ...     user = await store.get(key="user:123")

        With sanitization for long keys/collections:

        >>> async with S3Store(
        ...     bucket_name="my-kv-store",
        ...     collection_sanitization_strategy=S3CollectionSanitizationStrategy(),
        ...     key_sanitization_strategy=S3KeySanitizationStrategy(),
        ... ) as store:
        ...     await store.put(key="very_long_key" * 100, value={"data": "test"})

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
    _raw_client: Any
    _client: S3Client | None
    _owns_client: bool

    @overload
    def __init__(
        self,
        *,
        client: S3Client,
        bucket_name: str,
        default_collection: str | None = None,
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
    ) -> None:
        """Initialize the S3 store with a pre-configured client.

        Note: When you provide an existing client, you retain ownership and must manage
        its lifecycle yourself. The store will not close the client when the store is closed.

        Args:
            client: The S3 client to use. You must have entered the context manager before passing this in.
            bucket_name: The name of the S3 bucket to use.
            default_collection: The default collection to use if no collection is provided.
            collection_sanitization_strategy: Strategy for sanitizing collection names. Defaults to None (no sanitization).
            key_sanitization_strategy: Strategy for sanitizing keys. Defaults to None (no sanitization).
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
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
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
            collection_sanitization_strategy: Strategy for sanitizing collection names. Defaults to None (no sanitization).
            key_sanitization_strategy: Strategy for sanitizing keys. Defaults to None (no sanitization).
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
        collection_sanitization_strategy: SanitizationStrategy | None = None,
        key_sanitization_strategy: SanitizationStrategy | None = None,
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
            collection_sanitization_strategy: Strategy for sanitizing collection names. Defaults to None (no sanitization).
            key_sanitization_strategy: Strategy for sanitizing keys. Defaults to None (no sanitization).
        """
        self._bucket_name = bucket_name
        self._endpoint_url = endpoint_url

        if client:
            self._client = client
            self._raw_client = None
            self._owns_client = False
        else:
            session: Session = aioboto3.Session(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )

            self._raw_client = session.client(service_name="s3", endpoint_url=endpoint_url)  # pyright: ignore[reportUnknownMemberType]
            self._client = None
            self._owns_client = True

        super().__init__(
            default_collection=default_collection,
            collection_sanitization_strategy=collection_sanitization_strategy,
            key_sanitization_strategy=key_sanitization_strategy,
        )

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
        if self._owns_client and self._client:
            await self._client.__aexit__(exc_type, exc_value, traceback)

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
        if not self._client:
            if self._raw_client:
                self._client = await self._raw_client.__aenter__()

        from botocore.exceptions import ClientError

        try:
            await self._connected_client.head_bucket(Bucket=self._bucket_name)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if error_code in ("404", "NoSuchBucket") or http_status == HTTP_NOT_FOUND:
                import contextlib

                with contextlib.suppress(self._connected_client.exceptions.BucketAlreadyOwnedByYou):  # pyright: ignore[reportUnknownMemberType]
                    create_params: dict[str, Any] = {"Bucket": self._bucket_name}
                    region_name = getattr(self._connected_client.meta, "region_name", None)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                    # For regions other than us-east-1, specify LocationConstraint
                    # Skip for custom endpoints (LocalStack, MinIO) which may not support it
                    if region_name and region_name != "us-east-1" and not self._endpoint_url:
                        create_params["CreateBucketConfiguration"] = {"LocationConstraint": region_name}

                    await self._connected_client.create_bucket(**create_params)  # pyright: ignore[reportUnknownMemberType]
            else:
                raise

    def _get_s3_key(self, *, collection: str, key: str) -> str:
        """Generate the S3 object key for a given collection and key.

        The collection and key are sanitized using the configured sanitization strategies
        before being combined into the S3 object key format: {collection}/{key}.

        Args:
            collection: The collection name.
            key: The key within the collection.

        Returns:
            The S3 object key in format: {collection}/{key}
        """
        sanitized_collection, sanitized_key = self._sanitize_collection_and_key(collection=collection, key=key)
        return f"{sanitized_collection}/{sanitized_key}"

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

            async with response["Body"] as stream:  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                body_bytes = await stream.read()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            json_value = body_bytes.decode("utf-8")  # pyright: ignore[reportUnknownMemberType]

            managed_entry = self._serialization_adapter.load_json(json_str=json_value)

            if managed_entry.is_expired:
                await self._connected_client.delete_object(  # type: ignore[reportUnknownMemberType]
                    Bucket=self._bucket_name,
                    Key=s3_key,
                )
                return None
            return managed_entry  # noqa: TRY300

        except self._connected_client.exceptions.NoSuchKey:  # pyright: ignore[reportUnknownMemberType]
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
            await self._connected_client.head_object(  # pyright: ignore[reportUnknownMemberType]
                Bucket=self._bucket_name,
                Key=s3_key,
            )

        except ClientError as e:
            error = e.response.get("Error", {})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            metadata = e.response.get("ResponseMetadata", {})  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            error_code = error.get("Code", "")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            http_status = metadata.get("HTTPStatusCode", 0)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

            if error_code in ("404", "NoSuchKey") or http_status == HTTP_NOT_FOUND:
                return False

            if error_code in ("403", "AccessDenied"):
                await self._connected_client.delete_object(  # pyright: ignore[reportUnknownMemberType]
                    Bucket=self._bucket_name,
                    Key=s3_key,
                )
                return True

            raise

        await self._connected_client.delete_object(  # pyright: ignore[reportUnknownMemberType]
            Bucket=self._bucket_name,
            Key=s3_key,
        )
        return True

    @override
    async def _close(self) -> None:
        """Close the S3 client."""
        if self._owns_client and self._client:
            await self._client.__aexit__(None, None, None)
