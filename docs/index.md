# py-key-value Documentation

Welcome to the **py-key-value** documentation! This library provides a pluggable
interface for key-value stores with support for multiple backends, TTL handling,
type safety, and extensible wrappers.

## Overview

py-key-value is a Python framework that offers:

- **Multiple backends**: DynamoDB, Elasticsearch, Memcached, MongoDB, Redis,
  RocksDB, Valkey, and In-memory, Disk, etc.
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic model support, raise-on-missing behavior, etc.
- **Wrappers**: Statistics tracking, encryption, compression, and more
- **Collection-based**: Organize keys into logical collections/namespaces
- **Pluggable architecture**: Easy to add custom store implementations

## Quick Links

- [Getting Started](getting-started.md) - Installation and basic usage
- [Wrappers](wrappers.md) - Detailed documentation for all wrappers
- [Adapters](adapters.md) - Detailed documentation for all adapters
- [API Reference](api/protocols.md) - Complete API documentation

## Installation

Install the async library:

```bash
pip install py-key-value-aio
```

Install with specific backend support:

```bash
# Redis support
pip install py-key-value-aio[redis]

# DynamoDB support
pip install py-key-value-aio[dynamodb]

# All backends
pip install py-key-value-aio[all]
```

## Quick Example

```python
from key_value.aio.stores.memory import MemoryStore

# Create a store
store = MemoryStore()

# Store a value with TTL
await store.put(
    key="user:123",
    value={"name": "Alice", "email": "alice@example.com"},
    collection="users",
    ttl=3600  # 1 hour
)

# Retrieve the value
user = await store.get(key="user:123", collection="users")
print(user)  # {"name": "Alice", "email": "alice@example.com"}
```

## For Framework Authors

While key-value storage is valuable for individual projects, its true power
emerges when framework authors use it as a **pluggable abstraction layer**.

By coding your framework against the `AsyncKeyValue` protocol, you enable your
users to choose their own storage backend without changing a single line of your
framework code.

[Learn more about using py-key-value in your framework](getting-started.md#for-framework-authors)

## Project Links

- [GitHub Repository](https://github.com/strawgate/py-key-value)
- [PyPI Package](https://pypi.org/project/py-key-value-aio/)
- [Issue Tracker](https://github.com/strawgate/py-key-value/issues)

## License

This project is licensed under the Apache License 2.0.
