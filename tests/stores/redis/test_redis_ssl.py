import asyncio
import datetime
import ipaddress
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
import redis.exceptions
from redis.asyncio.client import Redis
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing_extensions import override

from key_value.aio._utils.wait import async_wait_for_true
from key_value.aio.stores.base import BaseStore
from key_value.aio.stores.redis import RedisStore
from tests.conftest import should_skip_docker_tests
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

REDIS_DB = 14  # Use a different database from the non-SSL tests
WAIT_FOR_REDIS_TIMEOUT = 30
REDIS_TLS_PORT = 6379

REDIS_SSL_IMAGE = "redis:7.0.0"


class RedisSSLFailedToStartError(Exception):
    pass


def _generate_self_signed_certs(cert_dir: str) -> tuple[str, str, str]:
    """Generate a self-signed CA, server cert, and server key for testing.

    Returns:
        Tuple of (ca_cert_path, server_cert_path, server_key_path).
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(server_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(
                ca_cert.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    cert_path = Path(cert_dir)
    ca_cert_path = cert_path / "ca.crt"
    server_cert_path = cert_path / "server.crt"
    server_key_path = cert_path / "server.key"

    ca_cert_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    server_cert_path.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    server_key_path.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )

    # Docker's redis user (uid 999) needs to read these files
    cert_path.chmod(0o755)
    server_key_path.chmod(0o644)
    server_cert_path.chmod(0o644)
    ca_cert_path.chmod(0o644)

    return str(ca_cert_path), str(server_cert_path), str(server_key_path)


def _make_redis_ssl_container(cert_dir: str) -> DockerContainer:
    """Create a Redis Docker container configured for TLS only (no plaintext port)."""
    return (
        DockerContainer(REDIS_SSL_IMAGE)
        .with_exposed_ports(REDIS_TLS_PORT)
        .with_volume_mapping(cert_dir, "/tls", mode="ro")
        .with_command(
            "redis-server "
            "--tls-port 6379 "
            "--port 0 "
            "--tls-cert-file /tls/server.crt "
            "--tls-key-file /tls/server.key "
            "--tls-ca-cert-file /tls/ca.crt "
            "--tls-auth-clients no"
        )
    )


def get_client_from_store(store: RedisStore) -> Redis:
    return store._client


@pytest.mark.skipif(should_skip_docker_tests(), reason="Docker is not running")
class TestRedisSSLStore(ContextManagerStoreTestMixin, BaseStoreTests):
    """Test RedisStore with TLS/SSL connections.

    Spins up a Redis 7 container configured for TLS-only (plaintext port
    disabled) with self-signed certificates, then runs the full base store
    test suite plus SSL-specific tests.
    """

    @pytest.fixture(autouse=True, scope="module")
    def ssl_certs(self) -> Iterator[tuple[str, str, str]]:
        """Generate self-signed certificates in a world-readable temp dir.

        We create our own temp directory under /tmp (instead of pytest's
        tmp_path_factory) because Docker needs to bind-mount the cert
        directory and the redis user (uid 999) inside the container must
        be able to read the files.
        """
        cert_dir = tempfile.mkdtemp(prefix="redis_ssl_certs_")
        Path(cert_dir).chmod(0o755)
        result = _generate_self_signed_certs(cert_dir)
        yield result
        shutil.rmtree(cert_dir, ignore_errors=True)

    @pytest.fixture(scope="module")
    def ca_cert_path(self, ssl_certs: tuple[str, str, str]) -> str:
        return ssl_certs[0]

    @pytest.fixture(autouse=True, scope="module")
    def redis_ssl_container(self, ssl_certs: tuple[str, str, str]):
        cert_dir = str(Path(ssl_certs[0]).parent)
        container = _make_redis_ssl_container(cert_dir)
        with container:
            wait_for_logs(container, "Ready to accept connections", timeout=WAIT_FOR_REDIS_TIMEOUT)
            yield container

    @pytest.fixture(scope="module")
    def redis_host(self, redis_ssl_container: DockerContainer) -> str:
        return redis_ssl_container.get_container_host_ip()

    @pytest.fixture(scope="module")
    def redis_port(self, redis_ssl_container: DockerContainer) -> int:
        return int(redis_ssl_container.get_exposed_port(REDIS_TLS_PORT))

    @pytest.fixture(autouse=True, scope="module")
    async def setup_redis(
        self,
        redis_ssl_container: DockerContainer,
        redis_host: str,
        redis_port: int,
    ) -> None:
        from key_value.aio.stores.redis.store import _create_redis_client

        async def ping_redis() -> bool:
            client = _create_redis_client(
                host=redis_host,
                port=redis_port,
                db=REDIS_DB,
                ssl_enabled=True,
                ssl_cert_reqs="none",
                ssl_check_hostname=False,
            )
            try:
                return await client.ping()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportGeneralTypeIssues]
            except Exception:
                return False
            finally:
                await client.aclose()

        if not await async_wait_for_true(bool_fn=ping_redis, tries=WAIT_FOR_REDIS_TIMEOUT, wait_time=1):
            msg = "Redis with SSL/TLS failed to start"
            raise RedisSSLFailedToStartError(msg)

    @override
    @pytest.fixture
    async def store(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ) -> RedisStore:
        """Create a Redis store with SSL for the base test suite."""
        redis_store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_cert_reqs="none",
            ssl_check_hostname=False,
        )
        _ = await get_client_from_store(store=redis_store).flushdb()  # pyright: ignore[reportUnknownMemberType]
        return redis_store

    # ── SSL-specific tests ──────────────────────────────────────────────

    async def test_ssl_rediss_url(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ):
        """Test that rediss:// URLs enable SSL automatically."""
        url = f"rediss://{redis_host}:{redis_port}/{REDIS_DB}"
        store = RedisStore(url=url, ssl_cert_reqs="none", ssl_check_hostname=False)
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="ssl_url_test", value={"ssl": True})
            result = await store.get(collection="test", key="ssl_url_test")
            assert result == {"ssl": True}
        finally:
            await store.close()

    async def test_ssl_host_port_with_params(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ):
        """Test SSL connection with host/port and ssl parameters."""
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_cert_reqs="none",
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="ssl_host_test", value={"mode": "host_port"})
            result = await store.get(collection="test", key="ssl_host_test")
            assert result == {"mode": "host_port"}
        finally:
            await store.close()

    async def test_ssl_rediss_url_without_explicit_params(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
        ca_cert_path: str,
    ):
        """Test that rediss:// URLs enable SSL without needing ssl=True.

        The ``rediss://`` scheme is sufficient to enable SSL. Here we pass
        the CA cert so verification succeeds against our self-signed server.
        """
        url = f"rediss://{redis_host}:{redis_port}/{REDIS_DB}"
        store = RedisStore(url=url, ssl_ca_certs=ca_cert_path, ssl_check_hostname=False)
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="rediss_default", value={"default_ssl": True})
            result = await store.get(collection="test", key="rediss_default")
            assert result == {"default_ssl": True}
        finally:
            await store.close()

    async def test_ssl_with_ca_verification(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
        ca_cert_path: str,
    ):
        """Test SSL connection with CA certificate verification.

        Uses the CA cert that signed the server's certificate. Hostname
        checking is disabled because testcontainers maps to a random port
        on the container host IP.
        """
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_ca_certs=ca_cert_path,
            ssl_check_hostname=False,
            ssl_cert_reqs="required",
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="ca_verify", value={"verified": True})
            result = await store.get(collection="test", key="ca_verify")
            assert result == {"verified": True}
        finally:
            await store.close()

    async def test_plaintext_connection_fails_against_tls_server(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ):
        """Test that a non-SSL connection to a TLS-only Redis server fails.

        A plaintext client connecting to a TLS-only server may either get an
        immediate ConnectionError or hang indefinitely, depending on the
        platform and Redis version. We use a short timeout to handle both.
        """
        store = RedisStore(host=redis_host, port=redis_port, db=REDIS_DB)
        try:
            with pytest.raises((redis.exceptions.ConnectionError, asyncio.TimeoutError, TimeoutError)):
                await asyncio.wait_for(
                    store.get(collection="test", key="should_fail"),
                    timeout=5.0,
                )
        finally:
            await store.close()

    async def test_ssl_auto_enabled_by_cert_params(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
        ca_cert_path: str,
    ):
        """Test that providing ssl_ca_certs implicitly enables SSL."""
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl_ca_certs=ca_cert_path,
            ssl_check_hostname=False,
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="auto_ssl", value={"auto": True})
            result = await store.get(collection="test", key="auto_ssl")
            assert result == {"auto": True}
        finally:
            await store.close()

    # ── Verification mode tests ───────────────────────────────────────

    async def test_cert_reqs_optional_with_ca(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
        ca_cert_path: str,
    ):
        """Test ssl_cert_reqs='optional' with a CA cert succeeds.

        In ``optional`` mode (:data:`ssl.CERT_OPTIONAL`) the server certificate
        is verified if presented. Since Redis always presents its cert, we must
        supply the CA so verification passes. The difference from ``required``
        is that ``optional`` would allow the connection even if the server
        chose not to present a certificate (which Redis never does).
        """
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_ca_certs=ca_cert_path,
            ssl_cert_reqs="optional",
            ssl_check_hostname=False,
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="optional_mode", value={"mode": "optional"})
            result = await store.get(collection="test", key="optional_mode")
            assert result == {"mode": "optional"}
        finally:
            await store.close()

    async def test_cert_reqs_required_fails_without_ca(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ):
        """Test ssl_cert_reqs='required' rejects self-signed certs when no CA is provided.

        Without a matching CA certificate the server's self-signed cert
        cannot be verified, so the connection must fail.
        """
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_cert_reqs="required",
            ssl_check_hostname=False,
        )
        try:
            with pytest.raises(redis.exceptions.ConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
                await store.get(collection="test", key="should_fail")
        finally:
            await store.close()

    async def test_cert_reqs_required_succeeds_with_ca(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
        ca_cert_path: str,
    ):
        """Test ssl_cert_reqs='required' succeeds when the correct CA is provided."""
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_ca_certs=ca_cert_path,
            ssl_cert_reqs="required",
            ssl_check_hostname=False,
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="required_ca", value={"mode": "required"})
            result = await store.get(collection="test", key="required_ca")
            assert result == {"mode": "required"}
        finally:
            await store.close()

    async def test_cert_reqs_none_ignores_invalid_cert(
        self,
        setup_redis: None,
        redis_host: str,
        redis_port: int,
    ):
        """Test ssl_cert_reqs='none' connects without any certificate validation."""
        store = RedisStore(
            host=redis_host,
            port=redis_port,
            db=REDIS_DB,
            ssl=True,
            ssl_cert_reqs="none",
            ssl_check_hostname=False,
        )
        try:
            _ = await get_client_from_store(store=store).flushdb()  # pyright: ignore[reportUnknownMemberType]
            await store.put(collection="test", key="none_mode", value={"mode": "none"})
            result = await store.get(collection="test", key="none_mode")
            assert result == {"mode": "none"}
        finally:
            await store.close()

    @pytest.mark.skip(reason="Distributed Caches are unbounded")
    @override
    async def test_not_unbounded(self, store: BaseStore): ...
