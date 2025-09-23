"""
Tests specific to the memory implementation.
"""

import pytest
import threading
import time

from kv_store_adapter.memory import MemoryKVStore


class TestMemoryKVStoreSpecific:
    """Tests specific to memory implementation."""

    def test_thread_safety(self):
        """Test that memory store is thread-safe."""
        store = MemoryKVStore()
        results = []
        
        def worker(thread_id):
            """Worker function for threading test."""
            for i in range(100):
                key = f"thread_{thread_id}_key_{i}"
                value = f"thread_{thread_id}_value_{i}"
                store.set(key, value)
                retrieved = store.get(key)
                results.append(retrieved == value)
        
        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All operations should have succeeded
        assert all(results)
        assert len(results) == 500  # 5 threads * 100 operations

    def test_memory_isolation_between_instances(self):
        """Test that different memory store instances are isolated."""
        store1 = MemoryKVStore()
        store2 = MemoryKVStore()
        
        store1.set("shared_key", "store1_value")
        store2.set("shared_key", "store2_value")
        
        assert store1.get("shared_key") == "store1_value"
        assert store2.get("shared_key") == "store2_value"