import pytest
from testcontainers.vault import VaultContainer
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from key_value.shared.wait import async_wait_for_true
from tests.conftest import should_skip_docker_tests
from tests.stores.base import (
    BaseStoreTests,
)

# Vault test configuration
VAULT_TOKEN = "dev-root-token"
VAULT_MOUNT_POINT = "secret"
VAULT_CONTAINER_PORT = 8200

WAIT_FOR_VAULT_TIMEOUT = 30

VAULT_VERSIONS_TO_TEST = [
    "1.12.0",  # Released Oct 2022
    "1.21.0",  # Released Oct 2025
]


class VaultFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
@pytest.mark.filterwarnings("ignore:A configured store is unstable and may change in a backwards incompatible way. Use at your own risk.")
class TestVaultStore(BaseStoreTests):
    def get_vault_client(self, vault_url: str):
        from key_value.aio.stores.vault.store import _create_vault_client

        return _create_vault_client(url=vault_url, token=VAULT_TOKEN)

    async def ping_vault(self, vault_url: str) -> bool:
        try:
            client = self.get_vault_client(vault_url)
            return client.sys.is_initialized()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        except Exception:
            return False

    @pytest.fixture(autouse=True, scope="module", params=VAULT_VERSIONS_TO_TEST)
    def vault_container(self, request: pytest.FixtureRequest):
        version = request.param
        container = VaultContainer(image=f"hashicorp/vault:{version}")
        container.with_env("VAULT_DEV_ROOT_TOKEN_ID", VAULT_TOKEN)
        container.with_env("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
        with container:
            yield container

    @pytest.fixture(scope="module")
    def vault_host(self, vault_container: VaultContainer) -> str:
        return vault_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def vault_port(self, vault_container: VaultContainer) -> int:
        return int(vault_container.get_exposed_port(VAULT_CONTAINER_PORT))

    @pytest.fixture(scope="module")
    def vault_url(self, vault_host: str, vault_port: int) -> str:
        return f"http://{vault_host}:{vault_port}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_vault(self, vault_container: VaultContainer, vault_url: str) -> None:
        if not await async_wait_for_true(bool_fn=lambda: self.ping_vault(vault_url), tries=WAIT_FOR_VAULT_TIMEOUT, wait_time=1):
            msg = "Vault failed to start"
            raise VaultFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_vault: None, vault_url: str):
        from key_value.aio.stores.vault import VaultStore
        from key_value.aio.stores.vault.store import _get_vault_kv_v2

        store: VaultStore = VaultStore(
            url=vault_url,
            token=VAULT_TOKEN,
            mount_point=VAULT_MOUNT_POINT,
        )

        # Clean up any existing data - best effort, ignore errors
        client = self.get_vault_client(vault_url)
        kv_v2 = _get_vault_kv_v2(client)
        try:
            # List all secrets and delete them
            secrets_list = kv_v2.list_secrets(path="", mount_point=VAULT_MOUNT_POINT)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if secrets_list and "data" in secrets_list and "keys" in secrets_list["data"]:
                for key in secrets_list["data"]["keys"]:  # pyright: ignore[reportUnknownVariableType]
                    # Best effort cleanup - ignore individual deletion failures
                    kv_v2.delete_metadata_and_all_versions(path=key.rstrip("/"), mount_point=VAULT_MOUNT_POINT)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        except Exception:  # noqa: S110
            # Cleanup is best-effort, ignore all errors
            pass

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
