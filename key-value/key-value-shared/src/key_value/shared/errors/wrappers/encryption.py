from key_value.shared.errors import KeyValueOperationError


class EncryptionError(KeyValueOperationError):
    """Exception raised when encryption or decryption fails."""


class DecryptionError(EncryptionError):
    """Exception raised when decryption fails."""
