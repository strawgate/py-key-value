from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from dirty_equals import IsFloat
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.couchbase import CouchbaseStore
from key_value.aio.stores.couchbase.store import (
    CouchbaseSerializationAdapter,
    CouchbaseV1CollectionSanitizationStrategy,
)
from key_value.shared.managed_entry import ManagedEntry
from key_value.shared.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

try:
    from acouchbase.cluster import Cluster as AsyncCluster
    from couchbase.auth import PasswordAuthenticator
    from couchbase.options import ClusterOptions
    from testcontainers.generic import DockerContainer  # pyright: ignore[reportAttributeAccessIssue, reportUnknownVariableType]
except ImportError:
    pytest.skip("Couchbase SDK or testcontainers not installed", allow_module_level=True)

# Couchbase test configuration
COUCHBASE_TEST_BUCKET = "default"
COUCHBASE_TEST_USER = "Administrator"
COUCHBASE_TEST_PASSWORD = "password"

WAIT_FOR_COUCHBASE_TIMEOUT = 120

COUCHBASE_VERSIONS_TO_TEST = [
    "7.6.3",  # Latest stable LTS version
]


async def ping_couchbase(connection_string: str, username: str, password: str) -> bool:
    try:
        auth = PasswordAuthenticator(username, password)
        cluster = AsyncCluster(connection_string, ClusterOptions(auth))
        bucket = cluster.bucket("default")  # pyright: ignore[reportUnknownMemberType]
        await bucket.on_connect()  # pyright: ignore[reportUnknownMemberType]
        # close() is synchronous in the acouchbase SDK
        cluster.close()  # pyright: ignore[reportUnusedCoroutine]
    except Exception:
        return False

    return True


class CouchbaseFailedToStartError(Exception):
    pass


def test_managed_entry_document_conversion():
    """Test that documents are stored as JSON dicts."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)

    adapter = CouchbaseSerializationAdapter()
    document = adapter.dump_dict(entry=managed_entry)

    assert document == snapshot(
        {
            "version": 1,
            "value": {"object": {"test": "test"}},
            "created_at": datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            "expires_at": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        }
    )

    round_trip_managed_entry = adapter.load_dict(data=document)

    assert round_trip_managed_entry.value == managed_entry.value
    assert round_trip_managed_entry.created_at == created_at
    assert round_trip_managed_entry.ttl == IsFloat(lt=0)
    assert round_trip_managed_entry.expires_at == expires_at


@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class BaseCouchbaseStoreTests(ContextManagerStoreTestMixin, BaseStoreTests):
    """Base class for Couchbase store tests."""

    @pytest.fixture(autouse=True, scope="module", params=COUCHBASE_VERSIONS_TO_TEST)
    def couchbase_container(self, request: pytest.FixtureRequest) -> Generator[Any, None, None]:
        version = request.param
        # Use the official Couchbase server image with community edition
        container: Any = (  # pyright: ignore[reportUnknownVariableType]
            DockerContainer(image=f"couchbase/server:{version}")  # pyright: ignore[reportUnknownMemberType]
            .with_exposed_ports(8091, 8092, 8093, 8094, 8095, 8096, 11210, 11211)
            .with_env("COUCHBASE_ADMINISTRATOR_USERNAME", COUCHBASE_TEST_USER)
            .with_env("COUCHBASE_ADMINISTRATOR_PASSWORD", COUCHBASE_TEST_PASSWORD)
        )
        with container:
            yield container

    @pytest.fixture(scope="module")
    def couchbase_connection_string(self, couchbase_container: Any) -> str:
        host = couchbase_container.get_container_host_ip()
        port = couchbase_container.get_exposed_port(11210)
        return f"couchbase://{host}:{port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_couchbase(self, couchbase_container: Any, couchbase_connection_string: str) -> None:
        # Wait for Couchbase to be ready
        if not await async_wait_for_true(
            bool_fn=lambda: ping_couchbase(couchbase_connection_string, COUCHBASE_TEST_USER, COUCHBASE_TEST_PASSWORD),
            tries=WAIT_FOR_COUCHBASE_TIMEOUT,
            wait_time=1,
        ):
            msg = "Couchbase failed to start"
            raise CouchbaseFailedToStartError(msg)

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    @override
    async def test_long_collection_name(self, store: CouchbaseStore, sanitizing_store: CouchbaseStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection" * 100, key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection" * 100, key="test_key") == {"test": "test"}

    @override
    async def test_special_characters_in_collection_name(self, store: CouchbaseStore, sanitizing_store: CouchbaseStore):  # pyright: ignore[reportIncompatibleMethodOverride]
        """Tests that special characters in the collection name will not raise an error."""
        with pytest.raises(Exception):  # noqa: B017, PT011
            await store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})

        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}

    async def test_couchbase_collection_name_sanitization(self, sanitizing_store: CouchbaseStore):
        """Tests that the V1 sanitization strategy produces the expected collection names."""
        await sanitizing_store.put(collection="test_collection!@#$%^&*()", key="test_key", value={"test": "test"})
        assert await sanitizing_store.get(collection="test_collection!@#$%^&*()", key="test_key") == {"test": "test"}


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
class TestCouchbaseStore(BaseCouchbaseStoreTests):
    """Test CouchbaseStore with native JSON storage."""

    @override
    @pytest.fixture
    async def store(self, setup_couchbase: None, couchbase_connection_string: str) -> CouchbaseStore:
        return CouchbaseStore(
            connection_string=couchbase_connection_string,
            username=COUCHBASE_TEST_USER,
            password=COUCHBASE_TEST_PASSWORD,
            bucket_name=COUCHBASE_TEST_BUCKET,
        )

    @pytest.fixture
    async def sanitizing_store(self, setup_couchbase: None, couchbase_connection_string: str) -> CouchbaseStore:
        return CouchbaseStore(
            connection_string=couchbase_connection_string,
            username=COUCHBASE_TEST_USER,
            password=COUCHBASE_TEST_PASSWORD,
            bucket_name=COUCHBASE_TEST_BUCKET,
            collection_sanitization_strategy=CouchbaseV1CollectionSanitizationStrategy(),
        )

    async def test_value_stored_as_json_dict(self, store: CouchbaseStore):
        """Verify values are stored as JSON dicts, not strings."""
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Verify we can retrieve it
        result = await store.get(collection="test", key="test_key")
        assert result == {"name": "Alice", "age": 30}
