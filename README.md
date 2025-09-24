# KV Store Adapter

A pluggable, async-first key-value store interface for Python applications with support for multiple backends and TTL (Time To Live) functionality.

## Features

- **Async-first**: Built from the ground up with `async`/`await` support
- **Multiple backends**: Redis, Elasticsearch, In-memory, Disk, and more
- **TTL support**: Automatic expiration handling across all store types
- **Type-safe**: Full type hints with Protocol-based interfaces
- **Adapters**: Pydantic, Single Collection, and more
- **Wrappers**: Statistics tracking and extensible wrapper system
- **Collection-based**: Organize keys into logical collections/namespaces
- **Pluggable architecture**: Easy to add custom store implementations

## Quick Start

```bash
pip install kv-store-adapter

# With specific backend support
pip install kv-store-adapter[redis]
pip install kv-store-adapter[elasticsearch]
pip install kv-store-adapter[memory]
pip install kv-store-adapter[disk]

# With all backends
pip install kv-store-adapter[memory,disk,redis,elasticsearch]
```

# The KV Store Protocol

The simplest way to get started is to use the `KVStoreProtocol` interface, which allows you to write code that works with any supported KV Store:

```python
from kv_store_adapter.types import KVStoreProtocol
from typing import Any

async def cache_user_data(store: KVStoreProtocol, user_id: str, data: dict[str, Any]) -> None:
    """Cache user data with 1-hour TTL."""
    await store.put("users", f"user:{user_id}", data, ttl=3600)

async def get_cached_user(store: KVStoreProtocol, user_id: str) -> dict[str, Any] | None:
    """Retrieve cached user data."""
    return await store.get("users", f"user:{user_id}")

# Works with any store implementation
from kv_store_adapter import RedisStore, MemoryStore

redis_store = RedisStore(url="redis://localhost:6379")
memory_store = MemoryStore(max_entries=1000)

# Same code works with both stores
await cache_user_data(redis_store, "123", {"name": "Alice"})
await cache_user_data(memory_store, "456", {"name": "Bob"})
```

## Store Implementations

Choose the store that best fits your needs. All stores implement the same `KVStoreProtocol` interface:

### Production Stores

- **RedisStore**: `RedisStore(url="redis://localhost:6379/0")`
- **ElasticsearchStore**: `ElasticsearchStore(url="https://localhost:9200", api_key="your-api-key")`
- **DiskStore**: A sqlite-based store for local persistence `DiskStore(path="./cache")`
- **MemoryStore**: A fast in-memory cache `MemoryStore()`

### Development/Testing Stores  

- **SimpleStore**: In-memory and inspectable for testing `SimpleStore()`
- **NullStore**: No-op store for testing `NullStore()`

For detailed configuration options and all available stores, see [DEVELOPING.md](DEVELOPING.md).

## Atomicity / Consistency

We strive to support atomicity and consistency across all stores and operations in the KVStoreProtocol. That being said,
there are operations available via the BaseKVStore class which are management operations like listing keys, listing collections, clearing collections,
culling expired entries, etc. These operations may not be atomic or may be eventually consistent across stores.

### TTL (Time To Live)

All stores support automatic expiration. Use TTL for session management, caching, and temporary data:

```python
from kv_store_adapter.types import KVStoreProtocol

async def session_example(store: KVStoreProtocol):
    # Store session with 1-hour expiration
    session_data = {"user_id": 123, "role": "admin"}
    await store.put("sessions", "session:abc123", session_data, ttl=3600)
    
    # Data automatically expires after 1 hour
    session = await store.get("sessions", "session:abc123")
    if session:
        print(f"Active session for user {session['user_id']}")
    else:
        print("Session expired or not found")
```

### Collections

Organize your data into logical namespaces:

```python
from kv_store_adapter.types import KVStoreProtocol

async def organize_data(store: KVStoreProtocol):
    # Same key in different collections - no conflicts
    await store.put("users", "123", {"name": "Alice", "email": "alice@example.com"})
    await store.put("products", "123", {"name": "Widget", "price": 29.99})
    await store.put("orders", "123", {"user_id": 456, "total": 99.99})
    
    # Work with specific collections
    user = await store.get("users", "123")
    product = await store.get("products", "123")
    
    # List all keys in a collection
    user_keys = await store.keys("users")
    print(f"User keys: {user_keys}")
```

## Adapters

The library provides an adapter pattern simplifying the user of the protocol/store. Adapters themselves do not implement the `KVStoreProtocol` interface and cannot be nested. Adapters can be used with wrappers and stores interchangeably.

The following adapters are available:

- **PydanticAdapter**: Converts data to and from a store using Pydantic models.
- **SingleCollectionAdapter**: Provides KV operations that do not require a collection parameter.

For example, the PydanticAdapter can be used to provide type-safe interactions with a store:

```python
from kv_store_adapter import PydanticAdapter, MemoryStore
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str

memory_store = MemoryStore()

user_adapter = PydanticAdapter(memory_store, User)

await user_adapter.put("users", "123", User(name="John Doe", email="john.doe@example.com"))
user: User | None = await user_adapter.get("users", "123")
```

## Wrappers

The library provides a wrapper pattern for adding functionality to a store. Wrappers themselves implement the `KVStoreProtocol` interface meaning that you can wrap any
store with any wrapper, and chain wrappers together as needed.

### Statistics Tracking

Track operation statistics for any store:

```python
from kv_store_adapter import StatisticsWrapper, MemoryStore

memory_store = MemoryStore()
store = StatisticsWrapper(memory_store)

# Use store normally - statistics are tracked automatically
await store.put("users", "123", {"name": "Alice"})
await store.get("users", "123")
await store.get("users", "456")  # Cache miss

# Access statistics
stats = store.statistics
user_stats = stats.get_collection("users")
print(f"Total gets: {user_stats.get.count}")
print(f"Cache hits: {user_stats.get.hit}")
print(f"Cache misses: {user_stats.get.miss}")
```

Other wrappers that are available include:

- **TTLClampWrapper**: Wraps a store and clamps the TTL to a given range.
- **PassthroughWrapper**: Wraps two stores, using the primary store as a write-through cache for the secondary store. For example, you could use a RedisStore as a distributed primary store and a MemoryStore as the cache store.
- **PrefixCollectionWrapper**: Wraps a store and prefixes all collections with a given prefix.
- **PrefixKeyWrapper**: Wraps a store and prefixes all keys with a given prefix.
- **SingleCollectionWrapper**: Wraps a store and forces all requests into a single collection.

See [DEVELOPING.md](DEVELOPING.md) for more information on how to create your own wrappers.

## Chaining Wrappers, Adapters, and Stores

Imagine you have a service where you want to cache 3 pydantic models in a single collection. You can do this by wrapping the store in a PydanticAdapter and a SingleCollectionAdapter:

```python
from kv_store_adapter import PydanticAdapter, SingleCollectionAdapter, MemoryStore
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str

store = MemoryStore()

users_store = PydanticAdapter(SingleCollectionWrapper(store, "users"), User)
products_store = PydanticAdapter(SingleCollectionWrapper(store, "products"), Product)
orders_store = PydanticAdapter(SingleCollectionWrapper(store, "orders"), Order)

await users_store.put("123", User(name="John Doe", email="john.doe@example.com"))
user: User | None = await users_store.get("123")
```

## Development

See [DEVELOPING.md](DEVELOPING.md) for development setup, testing, and contribution guidelines.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please read [DEVELOPING.md](DEVELOPING.md) for development setup and contribution guidelines.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.
