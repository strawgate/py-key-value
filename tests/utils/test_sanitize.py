import pytest
from inline_snapshot import snapshot

from key_value.shared.sanitize import (
    ALPHANUMERIC_CHARACTERS,
    LOWERCASE_ALPHABET,
    NUMBERS,
    UPPERCASE_ALPHABET,
    HashFragmentMode,
    sanitize_string,
)

ALWAYS_HASH = HashFragmentMode.ALWAYS
ONLY_IF_CHANGED_HASH = HashFragmentMode.ONLY_IF_CHANGED
NEVER_HASH = HashFragmentMode.NEVER


def test_sanitize_string():
    sanitized_string = sanitize_string(value="test string", max_length=16)
    assert sanitized_string == snapshot("test string")

    sanitized_string = sanitize_string(value="test string", max_length=16, hash_fragment_mode=ALWAYS_HASH)
    assert sanitized_string == snapshot("test st-d5579c46")

    sanitized_string = sanitize_string(value="test string", max_length=16, hash_fragment_mode=ONLY_IF_CHANGED_HASH)
    assert sanitized_string == snapshot("test string")

    sanitized_string = sanitize_string(value="test string", max_length=16, hash_fragment_mode=NEVER_HASH)
    assert sanitized_string == snapshot("test string")


@pytest.mark.parametrize(
    argnames=("hash_fragment_mode"),
    argvalues=[(ONLY_IF_CHANGED_HASH), (NEVER_HASH)],
)
@pytest.mark.parametrize(
    argnames=("max_length"),
    argvalues=[16, 32],
)
@pytest.mark.parametrize(
    argnames=("value", "allowed_chars"),
    argvalues=[
        ("test", None),
        ("test", "test"),
        ("test_test", "test_"),
        ("!@#$%^&*()", "!@#$%^&*()"),
        ("test", LOWERCASE_ALPHABET),
        ("test", ALPHANUMERIC_CHARACTERS),
    ],
)
def test_unchanged_strings(value: str, allowed_chars: str | None, max_length: int, hash_fragment_mode: HashFragmentMode):
    sanitized_string = sanitize_string(
        value=value, allowed_characters=allowed_chars, max_length=max_length, hash_fragment_mode=hash_fragment_mode
    )
    assert sanitized_string == value


@pytest.mark.parametrize(
    argnames=("hash_fragment_mode"),
    argvalues=[(ONLY_IF_CHANGED_HASH), (ALWAYS_HASH)],
)
def test_changed_strings(hash_fragment_mode: HashFragmentMode):
    def process_string(value: str, allowed_characters: str | None) -> str:
        return sanitize_string(value=value, allowed_characters=allowed_characters, max_length=16, hash_fragment_mode=hash_fragment_mode)

    sanitized_string = process_string(value="test", allowed_characters=NUMBERS)
    assert sanitized_string == snapshot("9f86d081")

    sanitized_string = process_string(value="test", allowed_characters=UPPERCASE_ALPHABET)
    assert sanitized_string == snapshot("9f86d081")

    sanitized_string = process_string(value="test with spaces", allowed_characters=LOWERCASE_ALPHABET)
    assert sanitized_string == snapshot("test_wi-ed2daf39")

    sanitized_string = process_string(value="test too long with spaces", allowed_characters=ALPHANUMERIC_CHARACTERS)
    assert sanitized_string == snapshot("test_to-479b94c3")

    sanitized_string = process_string(value="test too long with spaces", allowed_characters=None)
    assert sanitized_string == snapshot("test to-479b94c3")

    sanitized_string = process_string(value="test too long with spaces", allowed_characters=ALPHANUMERIC_CHARACTERS)
    assert sanitized_string == snapshot("test_to-479b94c3")

    sanitized_string = process_string(value="test way too long with spaces", allowed_characters=None)
    assert sanitized_string == snapshot("test wa-3d014b9b")

    sanitized_string = process_string(value="test way too long with spaces", allowed_characters=ALPHANUMERIC_CHARACTERS)
    assert sanitized_string == snapshot("test_wa-3d014b9b")
