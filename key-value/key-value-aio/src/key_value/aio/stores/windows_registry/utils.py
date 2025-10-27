import contextlib
import winreg

HiveType = int


def get_reg_sz_value(hive: HiveType, sub_key: str, value_name: str) -> str | None:
    """Retrieve a string value from the Windows Registry.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path.
        value_name: The name of the registry value to retrieve.

    Returns:
        The string value, or None if the key or value doesn't exist.
    """
    try:
        with winreg.OpenKey(key=hive, sub_key=sub_key) as reg_key:
            string, _ = winreg.QueryValueEx(reg_key, value_name)
            return string
    except (FileNotFoundError, OSError):
        return None


def set_reg_sz_value(hive: HiveType, sub_key: str, value_name: str, value: str) -> None:
    """Set a string value in the Windows Registry.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path.
        value_name: The name of the registry value to set.
        value: The string value to write.
    """
    with winreg.OpenKey(key=hive, sub_key=sub_key, access=winreg.KEY_WRITE) as reg_key:
        winreg.SetValueEx(reg_key, value_name, 0, winreg.REG_SZ, value)


def delete_reg_sz_value(hive: HiveType, sub_key: str, value_name: str) -> bool:
    """Delete a value from the Windows Registry.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path.
        value_name: The name of the registry value to delete.

    Returns:
        True if the value was deleted, False if it didn't exist or couldn't be deleted.
    """
    try:
        with winreg.OpenKey(key=hive, sub_key=sub_key, access=winreg.KEY_WRITE) as reg_key:
            winreg.DeleteValue(reg_key, value_name)
            return True
    except (FileNotFoundError, OSError):
        return False


def has_key(hive: HiveType, sub_key: str) -> bool:
    """Check if a registry key exists.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path to check.

    Returns:
        True if the key exists, False otherwise.
    """
    try:
        with winreg.OpenKey(key=hive, sub_key=sub_key):
            return True
    except (FileNotFoundError, OSError):
        return False


def create_key(hive: HiveType, sub_key: str) -> None:
    """Create a new registry key.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path to create.
    """
    winreg.CreateKey(hive, sub_key)


def delete_key(hive: HiveType, sub_key: str) -> bool:
    """Delete a registry key.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path to delete.

    Returns:
        True if the key was deleted, False if it didn't exist or couldn't be deleted.
    """
    try:
        with winreg.OpenKey(key=hive, sub_key=sub_key, access=winreg.KEY_WRITE) as reg_key:
            winreg.DeleteKey(reg_key, sub_key)
            return True
    except (FileNotFoundError, OSError):
        return False


def delete_sub_keys(hive: HiveType, sub_key: str) -> None:
    """Delete all subkeys of a registry key.

    This function recursively deletes all subkeys under the specified registry key.

    Args:
        hive: The registry hive (e.g., winreg.HKEY_CURRENT_USER).
        sub_key: The registry subkey path whose subkeys should be deleted.
    """
    try:
        with winreg.OpenKey(key=hive, sub_key=sub_key, access=winreg.KEY_WRITE | winreg.KEY_ENUMERATE_SUB_KEYS) as reg_key:
            index = 0
            while True:
                if not (next_child_key := winreg.EnumKey(reg_key, index)):
                    break

                with contextlib.suppress(Exception):
                    winreg.DeleteKey(reg_key, next_child_key)

                index += 1
    except (FileNotFoundError, OSError):
        return
