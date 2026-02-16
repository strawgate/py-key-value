import os
import uuid
import warnings
from collections.abc import Generator
from typing import Any

import pytest
from dirty_equals import IsStr
from inline_snapshot import snapshot
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from tests.conftest import should_skip_docker_tests
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

FIRESTORE_CONTAINER_PORT = 8080
FIRESTORE_WAIT_TIMEOUT = 30
FIRESTORE_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"


class FirestoreEmulatorFailedToStartError(Exception):
    pass


async def ping_firestore_emulator(emulator_host: str) -> bool:
    # Temporarily set the environment variable for the ping
    old_env = os.environ.get("FIRESTORE_EMULATOR_HOST")
    os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host
    try:
        client = AsyncClient(credentials=AnonymousCredentials())
        try:
            await client.collection("ping").document("ping").get()  # pyright: ignore[reportUnknownMemberType]
        except Exception:
            return False
        finally:
            client.close()
        return True
    finally:
        if old_env is not None:
            os.environ["FIRESTORE_EMULATOR_HOST"] = old_env
        elif "FIRESTORE_EMULATOR_HOST" in os.environ:
            del os.environ["FIRESTORE_EMULATOR_HOST"]


async def get_raw_document(*, emulator_host: str, project: str, collection: str, key: str) -> dict[str, Any] | None:
    old_env = os.environ.get("FIRESTORE_EMULATOR_HOST")
    os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host
    try:
        client = AsyncClient(project=project, credentials=AnonymousCredentials())
        try:
            snapshot = await client.collection(collection).document(key).get()  # pyright: ignore[reportUnknownMemberType]
            return snapshot.to_dict()
        finally:
            client.close()
    finally:
        if old_env is not None:
            os.environ["FIRESTORE_EMULATOR_HOST"] = old_env
        elif "FIRESTORE_EMULATOR_HOST" in os.environ:
            del os.environ["FIRESTORE_EMULATOR_HOST"]


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not available")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestFirestoreStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module")
    def firestore_container(self) -> Generator[DockerContainer, None, None]:
        container = DockerContainer(image=FIRESTORE_IMAGE)
        container.with_exposed_ports(FIRESTORE_CONTAINER_PORT)
        container.with_env("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
        container.with_command(f"gcloud emulators firestore start --host-port=0.0.0.0:{FIRESTORE_CONTAINER_PORT} --quiet")
        container.waiting_for(LogMessageWaitStrategy("Dev App Server is now running").with_startup_timeout(120))
        with container:
            yield container

    @pytest.fixture(scope="module")
    def emulator_host(self, firestore_container: DockerContainer) -> str:
        host = firestore_container.get_container_host_ip()
        port = firestore_container.get_exposed_port(FIRESTORE_CONTAINER_PORT)
        return f"{host}:{port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_firestore(self, firestore_container: DockerContainer, emulator_host: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: ping_firestore_emulator(emulator_host), tries=FIRESTORE_WAIT_TIMEOUT, wait_time=2):
            msg = "Firestore emulator failed to start"
            raise FirestoreEmulatorFailedToStartError(msg)

    @pytest.fixture
    def firestore_project(self) -> str:
        return f"firestore-project-{uuid.uuid4().hex}"

    @override
    @pytest.fixture
    async def store(self, setup_firestore: None, emulator_host: str, firestore_project: str) -> FirestoreStore:
        # Set the emulator host environment variable for the store
        os.environ["FIRESTORE_EMULATOR_HOST"] = emulator_host
        return FirestoreStore(credentials=AnonymousCredentials(), project=firestore_project, default_collection="test")

    @override
    @pytest.mark.skip(reason="Distributed cloud stores are unbounded")
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_firestore_document_format(self, store: FirestoreStore, emulator_host: str, firestore_project: str):
        await store.put(collection="test", key="document_format_test_1", value={"name": "Alice", "age": 30})

        raw_document = await get_raw_document(
            emulator_host=emulator_host, project=firestore_project, collection="test", key="document_format_test_1"
        )
        assert raw_document == snapshot(
            {
                "version": 1,
                "value": '{"age": 30, "name": "Alice"}',
                "created_at": IsStr(min_length=20, max_length=40),
            }
        )

        await store.put(collection="test", key="document_format_test_2", value={"name": "Bob", "age": 25}, ttl=10)
        raw_document = await get_raw_document(
            emulator_host=emulator_host, project=firestore_project, collection="test", key="document_format_test_2"
        )
        assert raw_document == snapshot(
            {
                "version": 1,
                "value": '{"age": 25, "name": "Bob"}',
                "created_at": IsStr(min_length=20, max_length=40),
                "expires_at": IsStr(min_length=20, max_length=40),
            }
        )
