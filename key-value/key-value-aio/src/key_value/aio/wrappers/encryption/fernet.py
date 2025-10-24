from cryptography.fernet import Fernet
from typing_extensions import overload

from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.wrappers.encryption.base import BaseEncryptionWrapper


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
        salt: str | None = None,
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
            fernet = Fernet(key=_generate_encryption_key(source_material=source_material, salt=salt))

        def encrypt_with_fernet(data: bytes) -> bytes:
            return fernet.encrypt(data)

        def decrypt_with_fernet(data: bytes) -> bytes:
            return fernet.decrypt(data)

        super().__init__(
            key_value=key_value,
            encryption_fn=encrypt_with_fernet,
            decryption_fn=decrypt_with_fernet,
            raise_on_decryption_error=raise_on_decryption_error,
        )


def _generate_encryption_key(source_material: str, salt: str | None = None) -> bytes:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    salt = salt or "py-key-value-salt"

    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt.encode(),
        info=b"Fernet",
    ).derive(key_material=source_material.encode())
