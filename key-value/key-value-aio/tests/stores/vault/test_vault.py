from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

import pytest
from inline_snapshot import snapshot
from key_value.shared.stores.wait import async_wait_for_true
from key_value.shared.utils.compound import compound_key
from key_value.shared.utils.managed_entry import ManagedEntry
from typing_extensions import override

from key_value.aio.stores.base import BaseStore
from tests.conftest import docker_container, should_skip_docker_tests
from tests.stores.base import (
    BaseStoreTests,
)

# Vault test configuration
VAULT_HOST = "localhost"
VAULT_PORT = 8200
VAULT_TOKEN = "dev-root-token"  # noqa: S105
VAULT_MOUNT_POINT = "secret"
VAULT_CONTAINER_PORT = 8200

WAIT_FOR_VAULT_TIMEOUT = 30

VAULT_VERSIONS_TO_TEST = [
    "1.12.0",  # Released Oct 2022
    "1.21.0",  # Released Oct 2025
]


def test_managed_entry_serialization():
    """Test ManagedEntry serialization to JSON for Vault storage."""
    created_at = datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    expires_at = created_at + timedelta(seconds=10)

    managed_entry = ManagedEntry(value={"test": "test"}, created_at=created_at, expires_at=expires_at)
    json_str = managed_entry.to_json()

    assert json_str == snapshot('{"value": {"test": "test"}}')

    round_trip_managed_entry = ManagedEntry.from_json(json_str=json_str)

    assert round_trip_managed_entry.value == managed_entry.value


class VaultFailedToStartError(Exception):
    pass


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestVaultStore(BaseStoreTests):
    def get_vault_client(self):
        import hvac

        return hvac.Client(url=f"http://{VAULT_HOST}:{VAULT_PORT}", token=VAULT_TOKEN)

    async def ping_vault(self) -> bool:
        try:
            client = self.get_vault_client()
            return client.sys.is_initialized()  # pyright: ignore[reportUnknownMemberType,reportUnknownReturnType,reportUnknownVariableType]
        except Exception:
            return False

    @pytest.fixture(scope="session", params=VAULT_VERSIONS_TO_TEST)
    async def setup_vault(self, request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
        version = request.param

        with docker_container(
            f"vault-test-{version}",
            f"hashicorp/vault:{version}",
            {str(VAULT_CONTAINER_PORT): VAULT_PORT},
            environment={
                "VAULT_DEV_ROOT_TOKEN_ID": VAULT_TOKEN,
                "VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
            },
        ):
            if not await async_wait_for_true(bool_fn=self.ping_vault, tries=WAIT_FOR_VAULT_TIMEOUT, wait_time=1):
                msg = f"Vault {version} failed to start"
                raise VaultFailedToStartError(msg)

            yield

    @override
    @pytest.fixture
    async def store(self, setup_vault: None):
        from key_value.aio.stores.vault import VaultStore

        store: VaultStore = VaultStore(
            url=f"http://{VAULT_HOST}:{VAULT_PORT}",
            token=VAULT_TOKEN,
            mount_point=VAULT_MOUNT_POINT,
        )

        # Clean up any existing data - best effort, ignore errors
        client = self.get_vault_client()
        try:
            # List all secrets and delete them
            secrets_list = client.secrets.kv.v2.list_secrets(path="", mount_point=VAULT_MOUNT_POINT)  # pyright: ignore[reportUnknownMemberType,reportUnknownReturnType,reportUnknownVariableType]
            if secrets_list and "data" in secrets_list and "keys" in secrets_list["data"]:
                for key in secrets_list["data"]["keys"]:  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                    # Best effort cleanup - ignore individual deletion failures
                    client.secrets.kv.v2.delete_metadata_and_all_versions(path=key.rstrip("/"), mount_point=VAULT_MOUNT_POINT)  # pyright: ignore[reportUnknownMemberType,reportUnknownReturnType,reportUnknownVariableType]
        except Exception:  # noqa: S110
            # Cleanup is best-effort, ignore all errors
            pass

        return store

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...

    async def test_value_stored_as_vault_secret(self, store):
        """Verify values are stored as Vault secrets with JSON in 'value' field."""
        from typing import TYPE_CHECKING

        if TYPE_CHECKING:
            from key_value.aio.stores.vault import VaultStore

        store: VaultStore  # type: ignore[name-defined]
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        # Get raw Vault secret using the compound key format
        combo_key = compound_key(collection="test", key="test_key")
        client = self.get_vault_client()
        response = client.secrets.kv.v2.read_secret(path=combo_key, mount_point=VAULT_MOUNT_POINT)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

        # Vault KV v2 returns data in response['data']['data']
        secret_data = response["data"]["data"]  # pyright: ignore[reportUnknownVariableType]

        assert secret_data == snapshot({"value": '{"value": {"name": "Alice", "age": 30}}'})  # pyright: ignore[reportUnknownArgumentType]
