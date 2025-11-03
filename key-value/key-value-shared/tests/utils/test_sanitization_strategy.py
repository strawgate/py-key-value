"""Tests for sanitization strategies."""

import pytest

from key_value.shared.errors import InvalidKeyError
from key_value.shared.utils.sanitization_strategy import (
    HASH_PREFIX,
    SANITIZE_PREFIX,
    CharacterSanitizationStrategy,
    HashLongKeysSanitizationStrategy,
    NoOpSanitizationStrategy,
    validate_no_reserved_prefixes,
)


class TestNoOpSanitizationStrategy:
    """Tests for NoOpSanitizationStrategy."""

    def test_sanitize_returns_unchanged(self) -> None:
        strategy = NoOpSanitizationStrategy()
        assert strategy.sanitize("my_key") == "my_key"
        assert strategy.sanitize("my::key!!") == "my::key!!"

    def test_validate_allows_all(self) -> None:
        strategy = NoOpSanitizationStrategy()
        # Should not raise
        strategy.validate("H_anything")
        strategy.validate("S_anything")
        strategy.validate("normal_key")


class TestHashLongKeysSanitizationStrategy:
    """Tests for HashLongKeysSanitizationStrategy."""

    def test_short_key_unchanged(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        short_key = "a" * 240
        assert strategy.sanitize(short_key) == short_key

    def test_long_key_hashed(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        long_key = "a" * 241
        sanitized = strategy.sanitize(long_key)

        # Should start with H_ prefix
        assert sanitized.startswith(HASH_PREFIX)
        # Total length should be 64 (H_ + 62 chars)
        assert len(sanitized) == 64
        # Should be deterministic
        assert strategy.sanitize(long_key) == sanitized

    def test_hash_is_deterministic(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=10)
        key = "this_is_a_very_long_key_that_will_be_hashed"

        hash1 = strategy.sanitize(key)
        hash2 = strategy.sanitize(key)

        assert hash1 == hash2

    def test_different_keys_produce_different_hashes(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=10)
        key1 = "this_is_a_very_long_key_1"
        key2 = "this_is_a_very_long_key_2"

        hash1 = strategy.sanitize(key1)
        hash2 = strategy.sanitize(key2)

        assert hash1 != hash2

    def test_validate_rejects_h_prefix(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=240)

        with pytest.raises(InvalidKeyError, match="reserved prefix 'H_'"):
            strategy.validate("H_abc123")

    def test_validate_allows_normal_keys(self) -> None:
        strategy = HashLongKeysSanitizationStrategy(max_length=240)

        # Should not raise
        strategy.validate("normal_key")
        strategy.validate("key_with_H_in_middle")
        strategy.validate("hkey")  # lowercase h is ok


class TestCharacterSanitizationStrategy:
    """Tests for CharacterSanitizationStrategy."""

    def test_unchanged_key_no_prefix(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz0123456789_-",
        )

        # Key with only allowed characters should be unchanged
        assert strategy.sanitize("my_key-123") == "my_key-123"

    def test_sanitized_key_has_prefix(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz0123456789_-",
        )

        # Key with invalid characters should get prefix
        sanitized = strategy.sanitize("my::key!!")
        assert sanitized.startswith(SANITIZE_PREFIX)

    def test_sanitization_is_deterministic(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz0123456789_-",
        )

        key = "my::invalid::key"
        result1 = strategy.sanitize(key)
        result2 = strategy.sanitize(key)

        assert result1 == result2

    def test_truncation_with_prefix(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=20,
            allowed_characters="abcdefghijklmnopqrstuvwxyz",
        )

        # Long key with invalid characters
        long_key = "a" * 100 + ":::"
        sanitized = strategy.sanitize(long_key)

        # Should not exceed max_length
        assert len(sanitized) <= 20
        # Should start with S_
        assert sanitized.startswith(SANITIZE_PREFIX)

    def test_validate_rejects_s_prefix(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz0123456789_-",
        )

        with pytest.raises(InvalidKeyError, match="reserved prefix 'S_'"):
            strategy.validate("S_abc123")

    def test_validate_allows_normal_keys(self) -> None:
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz0123456789_-",
        )

        # Should not raise
        strategy.validate("normal_key")
        strategy.validate("key_with_S_in_middle")
        strategy.validate("skey")  # lowercase s is ok


class TestCollisionPrevention:
    """Tests to ensure prefixes prevent collisions."""

    def test_user_key_cannot_collide_with_hash(self) -> None:
        """User cannot create a key that looks like a hashed key."""
        strategy = HashLongKeysSanitizationStrategy(max_length=10)

        long_key = "a" * 100
        hashed = strategy.sanitize(long_key)

        # Try to use the hashed value as a user key - should fail validation
        with pytest.raises(InvalidKeyError):
            strategy.validate(hashed)

    def test_user_key_cannot_collide_with_sanitized(self) -> None:
        """User cannot create a key that looks like a sanitized key."""
        strategy = CharacterSanitizationStrategy(
            max_length=100,
            allowed_characters="abcdefghijklmnopqrstuvwxyz",
        )

        key_with_special = "my::key"
        sanitized = strategy.sanitize(key_with_special)

        # Try to use the sanitized value as a user key - should fail validation
        with pytest.raises(InvalidKeyError):
            strategy.validate(sanitized)


class TestValidateNoReservedPrefixes:
    """Tests for the validate_no_reserved_prefixes helper."""

    def test_rejects_h_prefix(self) -> None:
        with pytest.raises(InvalidKeyError, match="reserved prefix 'H_'"):
            validate_no_reserved_prefixes("H_abc")

    def test_rejects_s_prefix(self) -> None:
        with pytest.raises(InvalidKeyError, match="reserved prefix 'S_'"):
            validate_no_reserved_prefixes("S_abc")

    def test_allows_normal_keys(self) -> None:
        # Should not raise
        validate_no_reserved_prefixes("normal_key")
        validate_no_reserved_prefixes("H")  # Just H is ok
        validate_no_reserved_prefixes("S")  # Just S is ok
        validate_no_reserved_prefixes("key_H_middle")
        validate_no_reserved_prefixes("key_S_middle")
