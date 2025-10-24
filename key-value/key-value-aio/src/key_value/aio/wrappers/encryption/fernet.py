from cryptography.fernet import Fernet
from key_value.shared.errors.wrappers.encryption import EncryptionVersionError
from typing_extensions import overload

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.encryption.base import BaseEncryptionWrapper

ENCRYPTION_VERSION = 1


class FernetEncryptionWrapper(BaseEncryptionWrapper):
    @overload
    def __init__(
        self,
        key_value: AsyncKeyValue,
        *,
        fernet: Fernet,
        raise_on_decryption_error: bool = True,
    ) -> None:
        """Initialize the Fernet encryption wrapper.

        Args:
            key_value: The key-value store to wrap.
            fernet: The Fernet instance to use for encryption and decryption.
            raise_on_decryption_error: Whether to raise an exception if decryption fails. Defaults to True.
        """

    @overload
    def __init__(
        self,
        key_value: AsyncKeyValue,
        *,
        source_material: str,
        salt: str,
        raise_on_decryption_error: bool = True,
    ) -> None:
        """Initialize the Fernet encryption wrapper.

        Args:
            key_value: The key-value store to wrap.
            source_material: The source material to combine with the salt to generate the encryption key.
            salt: The salt to combine with the source material to generate the encryption key. Defaults to "py-key-value-salt".
            raise_on_decryption_error: Whether to raise an exception if decryption fails. Defaults to True.
        """

    def __init__(
        self,
        key_value: AsyncKeyValue,
        *,
        fernet: Fernet | None = None,
        source_material: str | None = None,
        salt: str | None = None,
        raise_on_decryption_error: bool = True,
    ) -> None:
        if fernet is not None and source_material is not None:
            msg = "Cannot provide both fernet and source_material"
            raise ValueError(msg)

        if fernet is None:
            if source_material is None:
                msg = "Must provide either fernet or source_material"
                raise ValueError(msg)
            if salt is None:
                msg = "Must provide a salt"
                raise ValueError(msg)
            fernet = Fernet(key=_generate_encryption_key(source_material=source_material, salt=salt))

        def encrypt_with_fernet(data: bytes, encryption_version: int) -> bytes:
            if encryption_version > self.encryption_version:
                msg = f"Encryption failed: encryption version {encryption_version} is not supported"
                raise EncryptionVersionError(msg)
            return fernet.encrypt(data)

        def decrypt_with_fernet(data: bytes, encryption_version: int) -> bytes:
            if encryption_version > self.encryption_version:
                msg = f"Decryption failed: encryption version {encryption_version} is not supported"
                raise EncryptionVersionError(msg)
            return fernet.decrypt(data)

        super().__init__(
            key_value=key_value,
            encryption_fn=encrypt_with_fernet,
            decryption_fn=decrypt_with_fernet,
            encryption_version=ENCRYPTION_VERSION,
            raise_on_decryption_error=raise_on_decryption_error,
        )


def _generate_encryption_key(source_material: str, salt: str) -> bytes:
    import base64

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    derived_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt.encode(),
        info=b"Fernet",
    ).derive(key_material=source_material.encode())

    return base64.urlsafe_b64encode(derived_key)
