"""
Test configuration and fixtures.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from kv_store_adapter.memory import MemoryKVStore
from kv_store_adapter.disk import DiskKVStore


@pytest.fixture
def memory_store():
    """Create a fresh in-memory KV store for testing."""
    return MemoryKVStore()


@pytest.fixture
def disk_store():
    """Create a fresh disk-based KV store for testing."""
    temp_dir = tempfile.mkdtemp()
    store = DiskKVStore(temp_dir)
    yield store
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(params=["memory", "disk"])
def kv_store(request, memory_store, disk_store):
    """Parameterized fixture that tests both memory and disk implementations."""
    if request.param == "memory":
        return memory_store
    elif request.param == "disk":
        return disk_store