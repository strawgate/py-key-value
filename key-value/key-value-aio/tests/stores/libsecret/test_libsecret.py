import pytest
from typing_extensions import override

from key_value.aio.stores.libsecret.store import LibsecretStore
from tests.stores.base import BaseStoreTests

# Skip all tests if secretstorage is not available or DBus is not available
try:
    import secretstorage
    
    # Check if DBus is available
    try:
        conn = secretstorage.dbus_init()
        conn.close()
        DBUS_AVAILABLE = True
    except Exception:
        DBUS_AVAILABLE = False
except ImportError:
    DBUS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DBUS_AVAILABLE,
    reason="DBus or secretstorage not available"
)


class TestLibsecretStore(BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self) -> LibsecretStore:
        """Create a libsecret store for testing."""
        store = LibsecretStore(collection_name="py-key-value-test")
        
        # Clean up any existing test data
        await store.setup()
        await store.destroy()
        
        return store
