from collections.abc import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from typing_extensions import override
from upstash_redis.asyncio import Redis

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.upstash import UpstashRedisStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

WAIT_FOR_UPSTASH_TIMEOUT = 30

REDIS_VERSION = "7.0.0"

# Serverless Redis HTTP (SRH) serves the Upstash REST API in front of a regular Redis server.
SRH_VERSION = "0.0.10"
SRH_TOKEN = "test-token"


class UpstashFailedToStartError(Exception):
    pass


def get_client_from_store(store: UpstashRedisStore) -> Redis:
    return store._client


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestUpstashRedisStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @pytest.fixture(autouse=True, scope="module")
    def upstash_url(self) -> Generator[str, None, None]:
        with Network() as network:
            redis_container = DockerContainer(image=f"redis:{REDIS_VERSION}")
            _ = redis_container.with_network(network).with_network_aliases("redis")

            srh_container = DockerContainer(image=f"hiett/serverless-redis-http:{SRH_VERSION}")
            _ = srh_container.with_network(network).with_exposed_ports(80)
            _ = srh_container.with_env("SRH_MODE", "env")
            _ = srh_container.with_env("SRH_TOKEN", SRH_TOKEN)
            _ = srh_container.with_env("SRH_CONNECTION_STRING", "redis://redis:6379")

            with redis_container, srh_container:
                yield f"http://{srh_container.get_container_host_ip()}:{srh_container.get_exposed_port(80)}"

    @pytest.fixture(autouse=True, scope="module")
    async def setup_upstash(self, upstash_url: str) -> None:
        async def ping_upstash() -> bool:
            client = Redis(url=upstash_url, token=SRH_TOKEN)
            try:
                return await client.ping() == "PONG"
            except Exception:
                return False
            finally:
                await client.close()

        if not await async_wait_for_true(bool_fn=ping_upstash, tries=WAIT_FOR_UPSTASH_TIMEOUT, wait_time=1):
            msg = "Upstash Redis HTTP proxy failed to start"
            raise UpstashFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(self, setup_upstash: None, upstash_url: str) -> UpstashRedisStore:
        """Create an Upstash Redis store for testing."""
        upstash_store = UpstashRedisStore(url=upstash_url, token=SRH_TOKEN)
        _ = await get_client_from_store(store=upstash_store).flushdb()
        return upstash_store

    async def test_upstash_client_connection(self, setup_upstash: None, upstash_url: str):
        """Test Upstash Redis store creation with an existing client."""
        client = Redis(url=upstash_url, token=SRH_TOKEN)
        store = UpstashRedisStore(client=client)

        _ = await client.flushdb()
        await store.put(collection="test", key="client_test", value={"test": "value"}, ttl=10)
        assert await store.get(collection="test", key="client_test") == {"test": "value"}
        assert await client.ttl("test::client_test") > 0
        await client.close()

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
