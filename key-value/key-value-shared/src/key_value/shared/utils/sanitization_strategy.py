"""Sanitization strategy base class for key and collection name sanitization.

This module provides the SanitizationStrategy ABC that store implementations should use
to define their own sanitization strategy. Store-specific strategy implementations
should be defined within their respective store modules or use the provided strategies.

Sanitization strategies are responsible for:
1. Transforming keys/collections to meet backend requirements (length, characters, etc.)
2. Adding prefixes to prevent collisions between user keys and sanitized keys
3. Validating user input to reject reserved prefixes
4. Optionally attempting to recover original values (for debugging/enumeration)
"""

import hashlib
from abc import ABC, abstractmethod
from typing import Final

from key_value.shared.errors import InvalidKeyError
from key_value.shared.type_checking.bear_spray import bear_enforce
from key_value.shared.utils.sanitize import (
    DEFAULT_HASH_FRAGMENT_SEPARATOR,
    DEFAULT_REPLACEMENT_CHARACTER,
    HashFragmentMode,
    sanitize_string,
)

# Reserved prefixes for sanitized keys
HASH_PREFIX: Final[str] = "H_"
SANITIZE_PREFIX: Final[str] = "S_"
RESERVED_PREFIXES: Final[tuple[str, ...]] = (HASH_PREFIX, SANITIZE_PREFIX)


class SanitizationStrategy(ABC):
    """Base class for key/collection sanitization strategies.

    Sanitization strategies encapsulate the logic for transforming keys and collection
    names to meet backend storage requirements while preventing collisions between
    user-provided values and sanitized values.
    """

    @abstractmethod
    def sanitize(self, value: str) -> str:
        """Sanitize a key or collection name for storage.

        Args:
            value: The key or collection name to sanitize.

        Returns:
            The sanitized value, with appropriate prefixes if transformation occurred.
        """

    @abstractmethod
    def validate(self, value: str) -> None:
        """Validate that a user-provided value doesn't use reserved prefixes.

        Args:
            value: The user-provided key or collection name to validate.

        Raises:
            InvalidKeyError: If the value starts with a reserved prefix.
        """

    def try_unsanitize(self, value: str) -> str | None:  # noqa: ARG002
        """Attempt to recover the original value from a sanitized value.

        This is a best-effort operation and may return None if the original
        value cannot be recovered (e.g., for hashed values).

        Args:
            value: The sanitized value.

        Returns:
            The original value if recoverable, None otherwise.
        """
        return None


class NoOpSanitizationStrategy(SanitizationStrategy):
    """Sanitization strategy that performs no transformation.

    This strategy is used for stores that don't require any sanitization
    (e.g., MongoDB, which stores keys in document fields).
    """

    def sanitize(self, value: str) -> str:
        """Return the value unchanged."""
        return value

    def validate(self, value: str) -> None:
        """No validation needed for pass-through strategy."""


class HashLongKeysSanitizationStrategy(SanitizationStrategy):
    """Sanitization strategy that hashes keys exceeding a maximum length.

    This strategy uses SHA256 hashing for keys that exceed the specified maximum
    length. Hashed keys are prefixed with 'H_' to prevent collisions with
    user-provided keys.
    """

    def __init__(self, max_length: int = 240) -> None:
        """Initialize the hash-long-keys sanitization strategy.

        Args:
            max_length: Maximum allowed key length before hashing (default: 240).
        """
        self._max_length = max_length

    @bear_enforce
    def sanitize(self, value: str) -> str:
        """Hash the value if it exceeds the maximum length.

        Args:
            value: The key to sanitize.

        Returns:
            The original value if within max_length, or 'H_' + first 62 chars
            of SHA256 hash if it exceeds max_length (total 64 chars with prefix).
        """
        if len(value) <= self._max_length:
            return value

        sha256_hash: str = hashlib.sha256(value.encode()).hexdigest()
        # Reserve 2 chars for prefix, use 62 chars of hash (total 64)
        return f"{HASH_PREFIX}{sha256_hash[:62]}"

    @bear_enforce
    def validate(self, value: str) -> None:
        """Validate that the value doesn't start with the hash prefix.

        Args:
            value: The user-provided key to validate.

        Raises:
            InvalidKeyError: If the value starts with 'H_'.
        """
        if value.startswith(HASH_PREFIX):
            msg = f"Keys cannot start with reserved prefix '{HASH_PREFIX}' (reserved for hashed keys)"
            raise InvalidKeyError(message=msg)


class CharacterSanitizationStrategy(SanitizationStrategy):
    """Sanitization strategy that replaces invalid characters and truncates length.

    This strategy sanitizes values by:
    1. Replacing invalid characters with allowed ones
    2. Truncating to maximum length
    3. Adding a hash fragment if the value was modified
    4. Prefixing with 'S_' if sanitization occurred

    Values that are not modified are returned unchanged (no prefix).
    """

    def __init__(
        self,
        max_length: int,
        allowed_characters: str,
        replacement_character: str = DEFAULT_REPLACEMENT_CHARACTER,
        hash_fragment_separator: str = DEFAULT_HASH_FRAGMENT_SEPARATOR,
        hash_fragment_length: int = 8,
    ) -> None:
        """Initialize the character sanitization strategy.

        Args:
            max_length: Maximum allowed length (including prefix if added).
            allowed_characters: String of allowed characters.
            replacement_character: Character to replace invalid characters with.
            hash_fragment_separator: Separator between sanitized value and hash.
            hash_fragment_length: Length of hash fragment to append.
        """
        self._max_length = max_length
        self._allowed_characters = allowed_characters
        self._replacement_character = replacement_character
        self._hash_fragment_separator = hash_fragment_separator
        self._hash_fragment_length = hash_fragment_length

    @bear_enforce
    def sanitize(self, value: str) -> str:
        """Sanitize the value by replacing characters and adding hash if changed.

        Args:
            value: The value to sanitize.

        Returns:
            Original value if no changes needed, or 'S_' + sanitized value with
            hash fragment if modifications were made.
        """
        # First, try to sanitize without prefix to see if changes are needed
        sanitized = sanitize_string(
            value=value,
            max_length=self._max_length,
            allowed_characters=self._allowed_characters,
            replacement_character=self._replacement_character,
            hash_fragment_separator=self._hash_fragment_separator,
            hash_fragment_mode=HashFragmentMode.ONLY_IF_CHANGED,
            hash_fragment_length=self._hash_fragment_length,
        )

        # If no changes were made, return original
        if sanitized == value:
            return value

        # Changes were made, add prefix and re-sanitize with reduced max_length
        # to account for the prefix
        prefix_length = len(SANITIZE_PREFIX)
        adjusted_max_length = self._max_length - prefix_length

        sanitized = sanitize_string(
            value=value,
            max_length=adjusted_max_length,
            allowed_characters=self._allowed_characters,
            replacement_character=self._replacement_character,
            hash_fragment_separator=self._hash_fragment_separator,
            hash_fragment_mode=HashFragmentMode.ONLY_IF_CHANGED,
            hash_fragment_length=self._hash_fragment_length,
        )

        return f"{SANITIZE_PREFIX}{sanitized}"

    @bear_enforce
    def validate(self, value: str) -> None:
        """Validate that the value doesn't start with the sanitize prefix.

        Args:
            value: The user-provided value to validate.

        Raises:
            InvalidKeyError: If the value starts with 'S_'.
        """
        if value.startswith(SANITIZE_PREFIX):
            msg = f"Keys cannot start with reserved prefix '{SANITIZE_PREFIX}' (reserved for sanitized keys)"
            raise InvalidKeyError(message=msg)


def validate_no_reserved_prefixes(value: str) -> None:
    """Validate that a value doesn't start with any reserved prefix.

    This is a convenience function for stores that want to validate all
    reserved prefixes at once.

    Args:
        value: The user-provided value to validate.

    Raises:
        InvalidKeyError: If the value starts with any reserved prefix.
    """
    for prefix in RESERVED_PREFIXES:
        if value.startswith(prefix):
            msg = f"Keys cannot start with reserved prefix '{prefix}'"
            raise InvalidKeyError(message=msg)
