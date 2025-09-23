#!/usr/bin/env python3
"""
Example usage of the KV Store adapter implementations.

This script demonstrates the basic functionality of all three implementations:
- In-memory store
- Disk-based store  
- Redis store (if Redis is available)
"""

import tempfile
import shutil
from datetime import timedelta

from kv_store_adapter.memory import MemoryKVStore
from kv_store_adapter.disk import DiskKVStore


def demo_store(store, store_name):
    """Demonstrate basic functionality of a KV store."""
    print(f"\n=== {store_name} Demo ===")
    
    # Basic set/get operations
    store.set("user:1", {"name": "Alice", "age": 30})
    store.set("user:2", {"name": "Bob", "age": 25})
    
    print(f"Retrieved user:1: {store.get('user:1')}")
    print(f"Retrieved user:2: {store.get('user:2')}")
    
    # Namespace operations
    store.set("settings", "production", namespace="config")
    store.set("settings", "debug", namespace="test")
    
    print(f"Config settings: {store.get('settings', namespace='config')}")
    print(f"Test settings: {store.get('settings', namespace='test')}")
    
    # TTL operations
    store.set("session:abc123", {"user_id": 1}, ttl=timedelta(seconds=5))
    ttl_remaining = store.ttl("session:abc123")
    print(f"Session TTL remaining: {ttl_remaining:.2f} seconds")
    
    # Key listing
    print(f"All keys in default namespace: {store.keys()}")
    print(f"User keys: {store.keys(pattern='user:*')}")
    print(f"Config namespace keys: {store.keys(namespace='config')}")
    
    # Namespace listing
    print(f"Available namespaces: {store.list_namespaces()}")
    
    # Existence checks
    print(f"user:1 exists: {store.exists('user:1')}")
    print(f"user:3 exists: {store.exists('user:3')}")
    
    # Deletion
    deleted = store.delete("user:2")
    print(f"Deleted user:2: {deleted}")
    print(f"Keys after deletion: {store.keys()}")


def main():
    """Main demonstration function."""
    print("KV Store Adapter Demo")
    print("=====================")
    
    # Demo memory store
    memory_store = MemoryKVStore()
    demo_store(memory_store, "Memory Store")
    
    # Demo disk store
    temp_dir = tempfile.mkdtemp()
    try:
        disk_store = DiskKVStore(temp_dir)
        demo_store(disk_store, "Disk Store")
        
        print(f"\nDisk store data saved to: {temp_dir}")
        print("Files created:")
        import os
        for root, dirs, files in os.walk(temp_dir):
            level = root.replace(temp_dir, '').count(os.sep)
            indent = ' ' * 2 * level
            print(f"{indent}{os.path.basename(root)}/")
            subindent = ' ' * 2 * (level + 1)
            for file in files:
                print(f"{subindent}{file}")
    
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    # Demo Redis store (if available)
    try:
        from kv_store_adapter.redis import RedisKVStore
        
        # Try to create Redis store (will fail if Redis is not available)
        try:
            redis_store = RedisKVStore()
            demo_store(redis_store, "Redis Store")
        except (ImportError, ConnectionError) as e:
            print(f"\nRedis Store Demo skipped: {e}")
    
    except ImportError:
        print("\nRedis Store Demo skipped: redis package not installed")
    
    print("\nDemo completed!")


if __name__ == "__main__":
    main()