import contextlib
from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.s3 import S3Store
from key_value.shared.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

# S3 test configuration (using LocalStack)
S3_TEST_BUCKET = "kv-store-test"

WAIT_FOR_S3_TIMEOUT = 30

# LocalStack versions to test
LOCALSTACK_VERSIONS_TO_TEST = [
    "4.0.3",  # Latest stable version
]

LOCALSTACK_CONTAINER_PORT = 4566


async def ping_s3(endpoint_url: str) -> bool:
    """Check if LocalStack S3 is running."""
    from key_value.aio.stores.s3.store import _create_s3_client_context, _create_s3_session

    try:
        session = _create_s3_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_s3_client_context(session, endpoint_url=endpoint_url) as client:
            await client.list_buckets()
    except Exception:
        return False
    else:
        return True


class S3FailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestS3Store(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module", params=LOCALSTACK_VERSIONS_TO_TEST)
    def localstack_container(self, request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
        version = request.param
        container = DockerContainer(image=f"localstack/localstack:{version}")
        container.with_exposed_ports(LOCALSTACK_CONTAINER_PORT)
        container.with_env("SERVICES", "s3")
        container.waiting_for(LogMessageWaitStrategy("Ready."))
        with container:
            yield container

    @pytest.fixture(scope="module")
    def s3_host(self, localstack_container: DockerContainer) -> str:
        return localstack_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def s3_port(self, localstack_container: DockerContainer) -> int:
        return int(localstack_container.get_exposed_port(LOCALSTACK_CONTAINER_PORT))

    @pytest.fixture(scope="module")
    def s3_endpoint(self, s3_host: str, s3_port: int) -> str:
        return f"http://{s3_host}:{s3_port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_s3(self, localstack_container: DockerContainer, s3_endpoint: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_s3(s3_endpoint), tries=WAIT_FOR_S3_TIMEOUT, wait_time=1):
            msg = "LocalStack S3 failed to start"
            raise S3FailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_s3: None, s3_endpoint: str) -> S3Store:
        from key_value.aio.stores.s3 import S3CollectionSanitizationStrategy, S3KeySanitizationStrategy
        from key_value.aio.stores.s3.store import _create_s3_client_context, _create_s3_session

        store = S3Store(
            bucket_name=S3_TEST_BUCKET,
            endpoint_url=s3_endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            # Use sanitization strategies for tests to handle long collection/key names
            collection_sanitization_strategy=S3CollectionSanitizationStrategy(),
            key_sanitization_strategy=S3KeySanitizationStrategy(),
        )

        # Clean up test bucket if it exists
        session = _create_s3_session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        async with _create_s3_client_context(session, endpoint_url=s3_endpoint) as client:
            with contextlib.suppress(Exception):
                # Delete all objects in the bucket (handle pagination)
                continuation_token: str | None = None
                while True:
                    list_kwargs = {"Bucket": S3_TEST_BUCKET}
                    if continuation_token:
                        list_kwargs["ContinuationToken"] = continuation_token
                    response = await client.list_objects_v2(**list_kwargs)

                    # Delete objects from this page
                    for obj in response.get("Contents", []):
                        await client.delete_object(Bucket=S3_TEST_BUCKET, Key=obj["Key"])

                    # Check if there are more pages
                    continuation_token = response.get("NextContinuationToken")
                    if not continuation_token:
                        break

                # Delete the bucket
                await client.delete_bucket(Bucket=S3_TEST_BUCKET)

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
