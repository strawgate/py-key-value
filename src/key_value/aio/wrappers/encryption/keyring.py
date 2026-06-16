"""Keyring-based encryption wrapper that stores keys in the system keychain."""

from cryptography.fernet import Fernet, MultiFernet

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.encryption.base import BaseEncryptionWrapper
from key_value.shared.errors import EncryptionVersionError

try:
    import keyring
except ImportError as e:
    msg = "KeyringEncryptionWrapper requires py-key-value-aio[keyring]"
    raise ImportError(msg) from e

ENCRYPTION_VERSION = 1
DEFAULT_SERVICE_NAME = "py-key-value-encryption"
DEFAULT_KEY_NAME = "encryption-key"


class KeyringEncryptionWrapper(BaseEncryptionWrapper):
    """Wrapper that encrypts values using a Fernet key stored in the system keychain.

    This wrapper automatically generates and stores an encryption key in the system
    keychain on first use. Subsequent instantiations with the same service_name and
    key_name will retrieve the existing key.

    Key rotation is supported by providing old_keys parameter containing previously
    used Fernet keys. The wrapper will use MultiFernet to try decryption with each
    key in order.
    """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        *,
        service_name: str = DEFAULT_SERVICE_NAME,
        key_name: str = DEFAULT_KEY_NAME,
        old_keys: list[bytes] | None = None,
        raise_on_decryption_error: bool = True,
    ) -> None:
        """Initialize the keyring encryption wrapper.

        Args:
            key_value: The key-value store to wrap.
            service_name: The service name for keychain storage. Defaults to "py-key-value-encryption".
            key_name: The username/key name for keychain storage. Defaults to "encryption-key".
            old_keys: Optional list of old Fernet keys (as bytes) for rotation support.
                      These keys will be tried after the current key for decryption.
            raise_on_decryption_error: Whether to raise an exception if decryption fails. Defaults to True.
        """
        self._service_name = service_name
        self._key_name = key_name

        # Get or generate the encryption key
        key_str = keyring.get_password(service_name=service_name, username=key_name)
        if key_str is None:
            key = Fernet.generate_key()
            keyring.set_password(service_name=service_name, username=key_name, password=key.decode("ascii"))
        else:
            key = key_str.encode("ascii")

        # Build Fernet with optional old keys for rotation
        if old_keys:
            fernets = [Fernet(key), *[Fernet(old_key) for old_key in old_keys]]
            fernet: Fernet | MultiFernet = MultiFernet(fernets)
        else:
            fernet = Fernet(key)

        def encrypt_with_fernet(data: bytes) -> bytes:
            return fernet.encrypt(data)

        def decrypt_with_fernet(data: bytes, encryption_version: int) -> bytes:
            if encryption_version > ENCRYPTION_VERSION:
                msg = f"Decryption failed: encryption versions newer than {ENCRYPTION_VERSION} are not supported"
                raise EncryptionVersionError(msg)
            return fernet.decrypt(data)

        super().__init__(
            key_value=key_value,
            encryption_fn=encrypt_with_fernet,
            decryption_fn=decrypt_with_fernet,
            encryption_version=ENCRYPTION_VERSION,
            raise_on_decryption_error=raise_on_decryption_error,
        )
