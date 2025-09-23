# KV Store Adapter

A pluggable interface for Key-Value stores with multiple backend implementations.

## Overview

This package provides a common protocol for Key-Value store operations with support for:
- **Basic operations**: get, set, delete
- **TTL (Time To Live)**: Automatic expiration of keys
- **Namespaces/Collections**: Organize data into separate collections
- **Pattern matching**: Find keys using wildcard patterns
- **Multiple backends**: In-memory, disk-based, and Redis implementations

## Features

### Supported Operations
- `get(key, namespace=None)` - Retrieve a value
- `set(key, value, namespace=None, ttl=None)` - Store a value with optional TTL
- `delete(key, namespace=None)` - Delete a key
- `exists(key, namespace=None)` - Check if a key exists
- `ttl(key, namespace=None)` - Get remaining time-to-live
- `keys(namespace=None, pattern="*")` - List keys with pattern matching
- `clear_namespace(namespace)` - Clear all keys in a namespace
- `list_namespaces()` - List all available namespaces

### Backend Implementations

#### 1. Memory Store (`MemoryKVStore`)
- Fast in-memory storage using Python dictionaries
- Thread-safe with proper locking
- Data lost when process ends
- Perfect for caching and temporary storage

#### 2. Disk Store (`DiskKVStore`) 
- Persistent storage using the filesystem
- Each namespace is a directory
- Uses pickle for serialization and JSON for metadata
- Survives process restarts

#### 3. Redis Store (`RedisKVStore`)
- Uses Redis as the backend
- Leverages Redis's native TTL support
- Requires `redis` package and Redis server
- Scalable and production-ready

## Installation

```bash
# Basic installation
pip install kv-store-adapter

# With Redis support
pip install kv-store-adapter[redis]

# Development installation
pip install kv-store-adapter[dev]
```

## Quick Start

```python
from kv_store_adapter.memory import MemoryKVStore
from kv_store_adapter.disk import DiskKVStore
from kv_store_adapter.redis import RedisKVStore
from datetime import timedelta

# Memory store
store = MemoryKVStore()

# Disk store
store = DiskKVStore("/path/to/data")

# Redis store (requires Redis server)
store = RedisKVStore(host="localhost", port=6379)

# Basic operations
store.set("user:1", {"name": "Alice", "age": 30})
user = store.get("user:1")
print(user)  # {'name': 'Alice', 'age': 30}

# With TTL
store.set("session:abc", {"user_id": 1}, ttl=timedelta(hours=1))

# Using namespaces
store.set("config", "production", namespace="app")
store.set("config", "debug", namespace="test")

config = store.get("config", namespace="app")  # "production"

# Pattern matching
store.set("user:1", "Alice")
store.set("user:2", "Bob") 
store.set("admin:1", "Charlie")

users = store.keys(pattern="user:*")  # ["user:1", "user:2"]
```

## Detailed Examples

### Working with Namespaces

```python
from kv_store_adapter.memory import MemoryKVStore

store = MemoryKVStore()

# Store data in different namespaces
store.set("settings", {"theme": "dark"}, namespace="user:1")
store.set("settings", {"theme": "light"}, namespace="user:2")
store.set("cache", "some_value", namespace="temp")

# Retrieve from specific namespace
user1_settings = store.get("settings", namespace="user:1")

# List keys in a namespace
temp_keys = store.keys(namespace="temp")

# List all namespaces
namespaces = store.list_namespaces()
print(namespaces)  # ['user:1', 'user:2', 'temp']

# Clear a namespace
cleared_count = store.clear_namespace("temp")
```

### TTL and Expiration

```python
import time
from datetime import timedelta

# Set with TTL in seconds
store.set("session", {"user": "alice"}, ttl=30)

# Set with timedelta
store.set("cache", "data", ttl=timedelta(minutes=15))

# Check remaining TTL
remaining = store.ttl("session")
print(f"Session expires in {remaining:.2f} seconds")

# Key automatically expires
time.sleep(31)
exists = store.exists("session")  # False
```

### Error Handling

```python
from kv_store_adapter.exceptions import KeyNotFoundError

try:
    value = store.get("nonexistent_key")
except KeyNotFoundError as e:
    print(f"Key not found: {e}")

# Safe existence check
if store.exists("key"):
    value = store.get("key")
else:
    print("Key does not exist")
```

### Disk Store Persistence

```python
from kv_store_adapter.disk import DiskKVStore

# Create store with custom path
store1 = DiskKVStore("/my/data/path")
store1.set("persistent_key", "persistent_value")

# Data persists across instances
store2 = DiskKVStore("/my/data/path")
value = store2.get("persistent_key")  # "persistent_value"
```

### Redis Store Configuration

```python
from kv_store_adapter.redis import RedisKVStore

# Basic connection
store = RedisKVStore()

# Custom configuration
store = RedisKVStore(
    host="redis.example.com",
    port=6379,
    db=1,
    password="secret",
    socket_timeout=10
)

# All Redis connection parameters are supported
```

## Protocol Interface

All implementations follow the `KVStoreProtocol` interface:

```python
from abc import ABC, abstractmethod
from typing import Any, Optional, Union, List
from datetime import timedelta

class KVStoreProtocol(ABC):
    @abstractmethod
    def get(self, key: str, namespace: Optional[str] = None) -> Any: ...
    
    @abstractmethod
    def set(self, key: str, value: Any, namespace: Optional[str] = None, 
            ttl: Optional[Union[int, float, timedelta]] = None) -> None: ...
    
    @abstractmethod
    def delete(self, key: str, namespace: Optional[str] = None) -> bool: ...
    
    @abstractmethod
    def ttl(self, key: str, namespace: Optional[str] = None) -> Optional[float]: ...
    
    @abstractmethod
    def exists(self, key: str, namespace: Optional[str] = None) -> bool: ...
    
    @abstractmethod
    def keys(self, namespace: Optional[str] = None, pattern: str = "*") -> List[str]: ...
    
    @abstractmethod
    def clear_namespace(self, namespace: str) -> int: ...
    
    @abstractmethod
    def list_namespaces(self) -> List[str]: ...
```

## Thread Safety

- **MemoryKVStore**: Thread-safe using `threading.RLock`
- **DiskKVStore**: Thread-safe using `threading.RLock`  
- **RedisKVStore**: Thread-safe (Redis handles concurrency)

## Performance Characteristics

| Implementation | Speed | Persistence | Memory Usage | Scalability |
|----------------|-------|-------------|--------------|-------------|
| Memory         | Fastest | No | High | Single process |
| Disk           | Medium | Yes | Low | Single process |  
| Redis          | Fast | Yes | Medium | Multi-process |

## Testing

```bash
# Run all tests
pytest tests/

# Run specific implementation tests
pytest tests/test_memory_store.py
pytest tests/test_disk_store.py

# Run with coverage
pytest tests/ --cov=kv_store_adapter
```

## Development

```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e .[dev]

# Run tests
pytest

# Run example
python examples/demo.py
```

## Requirements

- Python >= 3.8
- `redis` package (optional, for Redis implementation)

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Changelog

### v0.1.0
- Initial release
- Memory, Disk, and Redis implementations
- Full protocol support with TTL and namespaces
- Comprehensive test suite
