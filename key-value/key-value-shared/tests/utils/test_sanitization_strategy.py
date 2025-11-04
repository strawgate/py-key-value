"""Tests for sanitization strategies."""

import pytest

from key_value.shared.errors.key_value import InvalidKeyError
from key_value.shared.utils.sanitization_strategy import (
    CharacterSanitizationStrategy,
    HashFragmentMode,
    HashLongKeysSanitizationStrategy,
    NoOpSanitizationStrategy,
)


class TestNoOpSanitizationStrategy:
    """Tests for NoOpSanitizationStrategy."""

    def test_sanitize_returns_unchanged(self) -> None:
        """Test that sanitize returns the value unchanged."""
        strategy = NoOpSanitizationStrategy()
        assert strategy.sanitize("test") == "test"
        assert strategy.sanitize("test::key::with::colons") == "test::key::with::colons"
        assert strategy.sanitize("a" * 1000) == "a" * 1000

    def test_validate_accepts_any_value(self) -> None:
        """Test that validate accepts any value."""
        strategy = NoOpSanitizationStrategy()
        strategy.validate("test")
        strategy.validate("H_something")
        strategy.validate("S_something")
        strategy.validate("")

    def test_try_unsanitize_returns_value(self) -> None:
        """Test that try_unsanitize returns the value."""
        strategy = NoOpSanitizationStrategy()
        assert strategy.try_unsanitize("test") == "test"


class TestHashLongKeysSanitizationStrategy:
    """Tests for HashLongKeysSanitizationStrategy."""

    def test_sanitize_short_key_unchanged(self) -> None:
        """Test that short keys are returned unchanged."""
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        short_key = "a" * 240
        assert strategy.sanitize(short_key) == short_key

    def test_sanitize_long_key_hashed(self) -> None:
        """Test that long keys are hashed with H_ prefix."""
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        long_key = "a" * 241
        sanitized = strategy.sanitize(long_key)

        assert sanitized.startswith("H_")
        assert len(sanitized) <= 240

    def test_sanitize_deterministic(self) -> None:
        """Test that sanitization is deterministic."""
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        long_key = "a" * 241

        result1 = strategy.sanitize(long_key)
        result2 = strategy.sanitize(long_key)

        assert result1 == result2

    def test_sanitize_different_keys_different_hashes(self) -> None:
        """Test that different keys produce different hashes."""
        strategy = HashLongKeysSanitizationStrategy(max_length=240)
        key1 = "a" * 241
        key2 = "b" * 241

        hash1 = strategy.sanitize(key1)
        hash2 = strategy.sanitize(key2)

        assert hash1 != hash2

    def test_validate_rejects_h_prefix(self) -> None:
        """Test that validate rejects keys starting with H_."""
        strategy = HashLongKeysSanitizationStrategy()

        with pytest.raises(InvalidKeyError, match="reserved prefixes"):
            strategy.validate("H_something")

    def test_validate_rejects_s_prefix(self) -> None:
        """Test that validate rejects keys starting with S_."""
        strategy = HashLongKeysSanitizationStrategy()

        with pytest.raises(InvalidKeyError, match="reserved prefixes"):
            strategy.validate("S_something")

    def test_validate_accepts_normal_keys(self) -> None:
        """Test that validate accepts normal keys."""
        strategy = HashLongKeysSanitizationStrategy()
        strategy.validate("test")
        strategy.validate("test_key")
        strategy.validate("test-key")

    def test_custom_max_length(self) -> None:
        """Test custom max_length parameter."""
        strategy = HashLongKeysSanitizationStrategy(max_length=100)
        long_key = "a" * 101

        sanitized = strategy.sanitize(long_key)
        assert sanitized.startswith("H_")
        assert len(sanitized) <= 100


class TestCharacterSanitizationStrategy:
    """Tests for CharacterSanitizationStrategy."""

    def test_sanitize_no_change_returns_original(self) -> None:
        """Test that unchanged values are returned as-is."""
        strategy = CharacterSanitizationStrategy()
        assert strategy.sanitize("test") == "test"
        assert strategy.sanitize("test_key") == "test_key"
        assert strategy.sanitize("test-key") == "test-key"

    def test_sanitize_replaces_invalid_characters(self) -> None:
        """Test that invalid characters are replaced."""
        strategy = CharacterSanitizationStrategy()
        sanitized = strategy.sanitize("test::key")

        assert sanitized.startswith("S_")
        assert "::" not in sanitized
        assert "test__key" in sanitized

    def test_sanitize_adds_hash_fragment(self) -> None:
        """Test that hash fragment is added when value changes."""
        strategy = CharacterSanitizationStrategy(hash_fragment_length=8)
        sanitized = strategy.sanitize("test::key")

        assert sanitized.startswith("S_")
        assert "-" in sanitized  # Hash separator
        # Format should be: S_test__key-<8-char-hash>
        assert len(sanitized.split("-")[-1]) == 8

    def test_sanitize_deterministic(self) -> None:
        """Test that sanitization is deterministic."""
        strategy = CharacterSanitizationStrategy()
        result1 = strategy.sanitize("test::key")
        result2 = strategy.sanitize("test::key")

        assert result1 == result2

    def test_sanitize_different_values_different_hashes(self) -> None:
        """Test that different values produce different hashes."""
        strategy = CharacterSanitizationStrategy()
        result1 = strategy.sanitize("test::key1")
        result2 = strategy.sanitize("test::key2")

        assert result1 != result2

    def test_sanitize_truncates_to_max_length(self) -> None:
        """Test that values are truncated to max_length."""
        strategy = CharacterSanitizationStrategy(max_length=50, hash_fragment_length=8)
        long_value = "a" * 200 + "::invalid"

        sanitized = strategy.sanitize(long_value)

        assert len(sanitized) <= 50
        assert sanitized.startswith("S_")

    def test_hash_fragment_mode_always(self) -> None:
        """Test ALWAYS mode adds hash even for unchanged values."""
        strategy = CharacterSanitizationStrategy(hash_fragment_mode=HashFragmentMode.ALWAYS)
        # This value doesn't need sanitization, but should still get hash
        sanitized = strategy.sanitize("test_key")

        assert sanitized.startswith("S_")
        assert "-" in sanitized

    def test_hash_fragment_mode_never(self) -> None:
        """Test NEVER mode doesn't add hash even for changed values."""
        strategy = CharacterSanitizationStrategy(hash_fragment_mode=HashFragmentMode.NEVER)
        sanitized = strategy.sanitize("test::key")

        # Should just be replaced characters, no prefix, no hash
        assert sanitized == "test__key"
        assert not sanitized.startswith("S_")
        assert "-" not in sanitized

    def test_hash_fragment_mode_only_if_changed(self) -> None:
        """Test ONLY_IF_CHANGED mode (default behavior)."""
        strategy = CharacterSanitizationStrategy(hash_fragment_mode=HashFragmentMode.ONLY_IF_CHANGED)

        # Unchanged - no hash
        unchanged = strategy.sanitize("test_key")
        assert unchanged == "test_key"
        assert not unchanged.startswith("S_")

        # Changed - has hash
        changed = strategy.sanitize("test::key")
        assert changed.startswith("S_")
        assert "-" in changed

    def test_validate_rejects_h_prefix(self) -> None:
        """Test that validate rejects keys starting with H_."""
        strategy = CharacterSanitizationStrategy()

        with pytest.raises(InvalidKeyError, match="reserved prefixes"):
            strategy.validate("H_something")

    def test_validate_rejects_s_prefix(self) -> None:
        """Test that validate rejects keys starting with S_."""
        strategy = CharacterSanitizationStrategy()

        with pytest.raises(InvalidKeyError, match="reserved prefixes"):
            strategy.validate("S_something")

    def test_validate_accepts_normal_keys(self) -> None:
        """Test that validate accepts normal keys."""
        strategy = CharacterSanitizationStrategy()
        strategy.validate("test")
        strategy.validate("test_key")
        strategy.validate("test-key")

    def test_custom_replacement_character(self) -> None:
        """Test custom replacement character."""
        strategy = CharacterSanitizationStrategy(replacement_character="-", hash_fragment_mode=HashFragmentMode.NEVER)
        sanitized = strategy.sanitize("test::key")

        assert sanitized == "test--key"

    def test_custom_allowed_characters(self) -> None:
        """Test custom allowed characters pattern."""
        # Only allow lowercase letters
        strategy = CharacterSanitizationStrategy(allowed_characters=r"[a-z]", hash_fragment_mode=HashFragmentMode.NEVER)
        sanitized = strategy.sanitize("Test123")

        assert "T" not in sanitized  # Uppercase removed
        assert "1" not in sanitized  # Numbers removed
        assert sanitized == "_est___"


class TestSanitizationStrategyEdgeCases:
    """Test edge cases and collision scenarios."""

    def test_collision_prevention_hash_strategy(self) -> None:
        """Test that H_ prefix prevents collisions."""
        strategy = HashLongKeysSanitizationStrategy(max_length=10)

        # User provides a short key that looks like a hash
        user_key = "H_abc123"

        # This should raise InvalidKeyError
        with pytest.raises(InvalidKeyError):
            strategy.validate(user_key)

    def test_collision_prevention_character_strategy(self) -> None:
        """Test that S_ prefix prevents collisions."""
        strategy = CharacterSanitizationStrategy()

        # User provides a key that looks sanitized
        user_key = "S_my_key-12345678"

        # This should raise InvalidKeyError
        with pytest.raises(InvalidKeyError):
            strategy.validate(user_key)

    def test_empty_string_handling(self) -> None:
        """Test handling of empty strings."""
        noop = NoOpSanitizationStrategy()
        hash_strategy = HashLongKeysSanitizationStrategy()
        char_strategy = CharacterSanitizationStrategy()

        assert noop.sanitize("") == ""
        assert hash_strategy.sanitize("") == ""
        assert char_strategy.sanitize("") == ""
