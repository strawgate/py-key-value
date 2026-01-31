import os
import uuid
import warnings
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from dirty_equals import IsStr
from inline_snapshot import snapshot
from key_value.shared.errors.store import StoreSetupError
from key_value.shared.stores.wait import async_wait_for_true
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

warnings.filterwarnings(
    "ignore",
    message=r"You are using a Python version .* google\.api_core",
    category=FutureWarning,
)

try:
    from google.auth.credentials import AnonymousCredentials
    from google.cloud.firestore import AsyncClient

    from key_value.aio.stores.firestore import FirestoreStore
except ImportError:  # pragma: no cover
    pytest.skip("Firestore dependencies not installed. Install with `py-key-value-aio[firestore]`.", allow_module_level=True)

FIRESTORE_HOST = "localhost"
FIRESTORE_PORT = 8080
FIRESTORE_EMULATOR_HOST = f"{FIRESTORE_HOST}:{FIRESTORE_PORT}"
FIRESTORE_WAIT_TIMEOUT = 30
FIRESTORE_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
FIRESTORE_COMMAND = f"gcloud emulators firestore start --host-port=0.0.0.0:{FIRESTORE_PORT} --quiet"
FIRESTORE_ENV = {"CLOUDSDK_CORE_DISABLE_PROMPTS": "1"}

os.environ.setdefault("FIRESTORE_EMULATOR_HOST", FIRESTORE_EMULATOR_HOST)


class FirestoreEmulatorFailedToStartError(Exception):
    pass


async def ping_firestore_emulator() -> bool:
    client = AsyncClient(credentials=AnonymousCredentials())
    try:
        await client.collection("ping").document("ping").get()  # pyright: ignore[reportUnknownMemberType]
    except Exception:
        return False
    finally:
        client.close()
    return True


async def get_raw_document(*, project: str, collection: str, key: str) -> dict[str, Any] | None:
    client = AsyncClient(project=project, credentials=AnonymousCredentials())
    try:
        snapshot = await client.collection(collection).document(key).get()  # pyright: ignore[reportUnknownMemberType]
        return snapshot.to_dict()
    finally:
        client.close()


@pytest.fixture(autouse=True, scope="session")
async def setup_firestore_emulator() -> AsyncGenerator[None, None]:
    with docker_container(
        "firestore-emulator-test",
        FIRESTORE_IMAGE,
        {str(FIRESTORE_PORT): FIRESTORE_PORT},
        environment=FIRESTORE_ENV,
        command=FIRESTORE_COMMAND,
    ):
        if not await async_wait_for_true(bool_fn=ping_firestore_emulator, tries=FIRESTORE_WAIT_TIMEOUT, wait_time=1):
            msg = "Firestore emulator failed to start"
            raise FirestoreEmulatorFailedToStartError(msg)

        yield


@pytest.fixture
async def ensure_emulator(setup_firestore_emulator: None) -> None:
    return None


@pytest.fixture
def firestore_project() -> str:
    return f"firestore-project-{uuid.uuid4().hex}"


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestFirestoreStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, ensure_emulator: None, firestore_project: str) -> FirestoreStore:
        return FirestoreStore(credentials=AnonymousCredentials(), project=firestore_project, default_collection="test")

    @override
    @pytest.mark.skip(reason="Distributed cloud stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_auto_create_false_raises_on_nonexistent_collection(self, ensure_emulator: None, firestore_project: str):
        """Test that auto_create=False raises StoreSetupError when collection doesn't exist."""
        store = FirestoreStore(credentials=AnonymousCredentials(), project=firestore_project, default_collection="test", auto_create=False)

        # Attempting to use the store should raise StoreSetupError (which wraps the ValueError)
        with pytest.raises(StoreSetupError, match="Collection 'nonexistent' does not exist"):
            async with store:
                await store.put(collection="nonexistent", key="test", value={"test": "value"})

    async def test_auto_create_false_works_with_existing_collection(self, ensure_emulator: None, firestore_project: str):
        """Test that auto_create=False works when collection already exists."""
        # First create a collection with auto_create=True
        store_creator = FirestoreStore(credentials=AnonymousCredentials(), project=firestore_project, default_collection="test")
        async with store_creator:
            await store_creator.put(collection="existing", key="test", value={"test": "value"})

        # Now use auto_create=False with the existing collection
        store = FirestoreStore(credentials=AnonymousCredentials(), project=firestore_project, default_collection="test", auto_create=False)
        async with store:
            # Should work fine since collection exists
            result = await store.get(collection="existing", key="test")
            assert result == {"test": "value"}

    async def test_firestore_document_format(self, store: FirestoreStore, firestore_project: str):
        await store.put(collection="test", key="document_format_test_1", value={"name": "Alice", "age": 30})

        raw_document = await get_raw_document(project=firestore_project, collection="test", key="document_format_test_1")
        assert raw_document == snapshot(
            {
                "version": 1,
                "value": '{"age": 30, "name": "Alice"}',
                "created_at": IsStr(min_length=20, max_length=40),
            }
        )

        await store.put(collection="test", key="document_format_test_2", value={"name": "Bob", "age": 25}, ttl=10)
        raw_document = await get_raw_document(project=firestore_project, collection="test", key="document_format_test_2")
        assert raw_document == snapshot(
            {
                "version": 1,
                "value": '{"age": 25, "name": "Bob"}',
                "created_at": IsStr(min_length=20, max_length=40),
                "expires_at": IsStr(min_length=20, max_length=40),
            }
        )
