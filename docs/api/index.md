# API Reference

Complete API reference documentation for py-key-value.

## Overview

The py-key-value API is organized into four main components:

- **[Protocols](protocols.md)** - Core interfaces that define the key-value store contract
- **[Stores](stores.md)** - Backend implementations for different storage systems
- **[Wrappers](wrappers.md)** - Decorators that add functionality to stores
- **[Adapters](adapters.md)** - Utilities that simplify working with stores

## Quick Links

### Core Protocols

The [`AsyncKeyValue`](protocols.md) protocol defines the async interface that all
stores implement.

The [`KeyValue`](protocols.md) protocol is the synchronous version.

### Popular Stores

- [MemoryStore](stores.md) - In-memory storage
- [RedisStore](stores.md) - Redis backend
- [DiskStore](stores.md) - File-based storage

### Common Wrappers

- [LoggingWrapper](wrappers.md) - Add logging to any store
- [CacheWrapper](wrappers.md) - Add caching layer
- [RetryWrapper](wrappers.md) - Add automatic retry logic

## Using the API Reference

Each page provides:

- **Type signatures** - Full type information for all parameters and return values
- **Docstrings** - Detailed descriptions of functionality
- **Source links** - View the implementation on GitHub
- **Cross-references** - Navigate between related components

## Example Usage

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper

# Create a store with logging
store = LoggingWrapper(MemoryStore())

# Use the store
await store.put("key", "value")
result = await store.get("key")
```

For more examples and guides, see the [User Guide](../stores.md).
