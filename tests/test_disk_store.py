"""
Tests specific to the disk implementation.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from kv_store_adapter.disk import DiskKVStore


class TestDiskKVStoreSpecific:
    """Tests specific to disk implementation."""

    def test_persistence_across_instances(self):
        """Test that data persists across different store instances."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create first store instance and add data
            store1 = DiskKVStore(temp_dir)
            store1.set("persistent_key", "persistent_value")
            store1.set("ns_key", "ns_value", namespace="test_ns")
            
            # Create second store instance with same directory
            store2 = DiskKVStore(temp_dir)
            
            # Data should be available in second instance
            assert store2.get("persistent_key") == "persistent_value"
            assert store2.get("ns_key", namespace="test_ns") == "ns_value"
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_file_structure(self):
        """Test that the correct file structure is created."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            store = DiskKVStore(temp_dir)
            store.set("test_key", "test_value")
            store.set("ns_key", "ns_value", namespace="test_ns")
            
            base_path = Path(temp_dir)
            
            # Check default namespace files
            default_ns = base_path / "default"
            assert default_ns.exists()
            assert (default_ns / "test_key.data").exists()
            assert (default_ns / "test_key.meta").exists()
            
            # Check custom namespace files
            test_ns = base_path / "test_ns"
            assert test_ns.exists()
            assert (test_ns / "ns_key.data").exists()
            assert (test_ns / "ns_key.meta").exists()
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_corrupted_files_handling(self):
        """Test handling of corrupted data files."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            store = DiskKVStore(temp_dir)
            store.set("test_key", "test_value")
            
            # Corrupt the data file
            data_file = Path(temp_dir) / "default" / "test_key.data"
            with open(data_file, 'w') as f:
                f.write("corrupted data")
            
            # Should raise KeyNotFoundError due to pickle error
            with pytest.raises(Exception):  # Could be KeyNotFoundError or pickle error
                store.get("test_key")
                
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)