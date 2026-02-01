# Getting Started

This guide will help you get started with py-key-value, from installation to
basic usage patterns.

## Installation

### Basic Installation

Install the async library:

```bash
pip install py-key-value-aio
```

### Backend-Specific Installation

Install with specific backend support:

```bash
# Redis support
pip install py-key-value-aio[redis]

# DynamoDB support
pip install py-key-value-aio[dynamodb]

# Elasticsearch support
pip install py-key-value-aio[elasticsearch]

# MongoDB support
pip install py-key-value-aio[mongodb]

# Firestore support
pip install py-key-value-aio[firestore]

# All backends
pip install py-key-value-aio[all]
```

## Basic Usage

### Creating a Store

The simplest way to get started is with the `MemoryStore`:

```python
from key_value.aio.stores.memory import MemoryStore

store = MemoryStore()
```

### Storing Values

Store a value with the `put` method:

```python
await store.put(
    key="user:123",
    value={"name": "Alice", "email": "alice@example.com"},
    collection="users"
)
```

### Retrieving Values

Retrieve a value with the `get` method:

```python
user = await store.get(key="user:123", collection="users")
print(user)  # {"name": "Alice", "email": "alice@example.com"}
```

### TTL Support

Set a TTL (time-to-live) for automatic expiration:

```python
await store.put(
    key="session:abc",
    value={"user_id": "123", "expires": "2024-01-01"},
    collection="sessions",
    ttl=3600  # Expires in 1 hour
)
```

### Deleting Values

Delete a value with the `delete` method:

```python
deleted = await store.delete(key="user:123", collection="users")
print(deleted)  # True if the value was deleted, False otherwise
```

## Working with Collections

Collections allow you to organize keys into logical namespaces:

```python
# Store users in the "users" collection
await store.put(
    key="alice",
    value={"name": "Alice"},
    collection="users"
)

# Store sessions in the "sessions" collection
await store.put(
    key="session-1",
    value={"user_id": "alice"},
    collection="sessions"
)
```

## Batch Operations

Perform batch operations for better performance:

```python
# Put multiple values at once
await store.put_many(
    keys=["user:1", "user:2", "user:3"],
    values=[
        {"name": "Alice"},
        {"name": "Bob"},
        {"name": "Charlie"}
    ],
    collection="users"
)

# Get multiple values at once
users = await store.get_many(
    keys=["user:1", "user:2", "user:3"],
    collection="users"
)

# Delete multiple values at once
count = await store.delete_many(
    keys=["user:1", "user:2", "user:3"],
    collection="users"
)
```

## Using Different Backends

### Redis

```python
from key_value.aio.stores.redis import RedisStore

store = RedisStore(url="redis://localhost:6379/0")
```

### DynamoDB

```python
from key_value.aio.stores.dynamodb import DynamoDBStore

store = DynamoDBStore(
    table_name="my-kv-store",
    region_name="us-east-1"
)
```

### Disk Storage

```python
from key_value.aio.stores.disk import DiskStore

store = DiskStore(directory="/path/to/storage")
```

## Using Wrappers

Wrappers add functionality to stores. They can be stacked for combined effects:

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper
from key_value.aio.wrappers.statistics import StatisticsWrapper

# Create a store with logging and statistics
store = StatisticsWrapper(
    LoggingWrapper(
        MemoryStore()
    )
)

# Use the store normally
await store.put(key="test", value={"data": "value"})

# Access statistics
stats = store.get_statistics()
print(f"Total puts: {stats.put.count}")
```

See the [Wrappers](wrappers.md) page for detailed documentation on all available
wrappers.

## Using Adapters

Adapters provide specialized interfaces for specific use cases:

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.pydantic import PydanticAdapter

class User(BaseModel):
    name: str
    email: str

# Create a type-safe adapter
adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=User
)

# Store and retrieve type-safe models
await adapter.put(
    key="user:123",
    value=User(name="Alice", email="alice@example.com"),
    collection="users"
)

user = await adapter.get(key="user:123", collection="users")
print(user.name)  # Type-safe access: "Alice"
```

See the [Adapters](adapters.md) page for detailed documentation on all available
adapters.

## For Framework Authors

If you're building a framework, you can use the `AsyncKeyValue` protocol to
allow your users to plug in any backend:

```python
from key_value.aio.protocols.key_value import AsyncKeyValue

class YourFramework:
    def __init__(self, cache: AsyncKeyValue):
        self.cache = cache

    async def store_session(self, session_id: str, data: dict):
        await self.cache.put(
            key=f"session:{session_id}",
            value=data,
            collection="sessions",
            ttl=3600
        )

    async def get_session(self, session_id: str):
        return await self.cache.get(
            key=f"session:{session_id}",
            collection="sessions"
        )
```

Your users can then choose their own backend:

```python
from your_framework import YourFramework
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.stores.redis import RedisStore

# Development
framework = YourFramework(cache=MemoryStore())

# Production
framework = YourFramework(
    cache=RedisStore(url="redis://localhost:6379/0")
)
```

## Next Steps

- Learn about all available [Wrappers](wrappers.md)
- Learn about all available [Adapters](adapters.md)
- Explore the [API Reference](api/protocols.md)
