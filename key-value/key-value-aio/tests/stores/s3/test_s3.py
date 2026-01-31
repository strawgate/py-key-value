import contextlib
from collections.abc import AsyncGenerator

import pytest
from typing_extensions import override

from key_value.aio._shared.stores.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.s3 import S3Store
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# S3 test configuration (using LocalStack)
S3_HOST = "localhost"
S3_HOST_PORT = 4566
S3_ENDPOINT = f"http://{S3_HOST}:{S3_HOST_PORT}"
S3_TEST_BUCKET = "kv-store-test"

WAIT_FOR_S3_TIMEOUT = 30

# LocalStack versions to test
LOCALSTACK_VERSIONS_TO_TEST = [
    "4.0.3",  # Latest stable version
]

LOCALSTACK_CONTAINER_PORT = 4566


async def ping_s3() -> bool:
    """Check if LocalStack S3 is running."""
    try:
        import aioboto3

        session = aioboto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with session.client(service_name="s3", endpoint_url=S3_ENDPOINT) as client:  # type: ignore
            await client.list_buckets()  # type: ignore
    except Exception:
        return False
    else:
        return True


class S3FailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestS3Store(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(scope="session", params=LOCALSTACK_VERSIONS_TO_TEST)
    async def setup_s3(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        # LocalStack container for S3
        with docker_container(
            f"s3-test-{version}",
            f"localstack/localstack:{version}",
            {str(LOCALSTACK_CONTAINER_PORT): S3_HOST_PORT},
            environment={"SERVICES": "s3"},
        ):
            if not await async_wait_for_true(bool_fn=ping_s3, tries=WAIT_FOR_S3_TIMEOUT, wait_time=2):
                msg = f"LocalStack S3 {version} failed to start"
                raise S3FailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_s3: None) -> S3Store:
        from key_value.aio.stores.s3 import S3CollectionSanitizationStrategy, S3KeySanitizationStrategy

        store = S3Store(
            bucket_name=S3_TEST_BUCKET,
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            # Use sanitization strategies for tests to handle long collection/key names
            collection_sanitization_strategy=S3CollectionSanitizationStrategy(),
            key_sanitization_strategy=S3KeySanitizationStrategy(),
        )

        # Clean up test bucket if it exists
        import aioboto3

        session = aioboto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with session.client(service_name="s3", endpoint_url=S3_ENDPOINT) as client:  # type: ignore
            with contextlib.suppress(Exception):
                # Delete all objects in the bucket (handle pagination)
                continuation_token: str | None = None
                while True:
                    list_kwargs = {"Bucket": S3_TEST_BUCKET}
                    if continuation_token:
                        list_kwargs["ContinuationToken"] = continuation_token
                    response = await client.list_objects_v2(**list_kwargs)  # type: ignore

                    # Delete objects from this page
                    for obj in response.get("Contents", []):  # type: ignore
                        await client.delete_object(Bucket=S3_TEST_BUCKET, Key=obj["Key"])  # type: ignore

                    # Check if there are more pages
                    continuation_token = response.get("NextContinuationToken")  # type: ignore
                    if not continuation_token:
                        break

                # Delete the bucket
                await client.delete_bucket(Bucket=S3_TEST_BUCKET)  # type: ignore

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
