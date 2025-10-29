# Wrappers

Wrappers are a powerful feature of py-key-value that allow you to add
functionality to any key-value store. Wrappers implement the `AsyncKeyValue`
protocol, so they can be used anywhere a store can be used.

## Available Wrappers

| Wrapper | Description |
|---------|-------------|
| [CollectionRoutingWrapper](#collectionroutingwrapper) | Route operations to different stores based on collection name |
| [CompressionWrapper](#compressionwrapper) | Compress values before storing and decompress on retrieval |
| [DefaultValueWrapper](#defaultvaluewrapper) | Return a default value when key is missing |
| [FernetEncryptionWrapper](#fernetencryptionwrapper) | Encrypt values before storing and decrypt on retrieval |
| [FallbackWrapper](#fallbackwrapper) | Fallback to a secondary store when the primary store fails |
| [LimitSizeWrapper](#limitsizewrapper) | Limit the size of entries stored in the cache |
| [LoggingWrapper](#loggingwrapper) | Log the operations performed on the store |
| [PassthroughCacheWrapper](#passthroughcachewrapper) | Wrap two stores to provide a read-through cache |
| [PrefixCollectionsWrapper](#prefixcollectionswrapper) | Prefix all collections with a given prefix |
| [PrefixKeysWrapper](#prefixkeyswrapper) | Prefix all keys with a given prefix |
| [ReadOnlyWrapper](#readonlywrapper) | Prevent all write operations on the underlying store |
| [RetryWrapper](#retrywrapper) | Retry failed operations with exponential backoff |
| [RoutingWrapper](#routingwrapper) | Route operations to different stores based on a routing function |
| [SingleCollectionWrapper](#singlecollectionwrapper) | Wrap a store to only use a single collection |
| [StatisticsWrapper](#statisticswrapper) | Track operation statistics for the store |
| [TimeoutWrapper](#timeoutwrapper) | Add timeout protection to store operations |
| [TTLClampWrapper](#ttlclampwrapper) | Clamp the TTL to a given range |

## What Are Wrappers?

Wrappers follow the decorator pattern - they wrap around a key-value store and
intercept operations to add additional behavior. Multiple wrappers can be
stacked to combine their effects.

### Wrapper Pattern Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper
from key_value.aio.wrappers.statistics import StatisticsWrapper

# Stack wrappers to combine functionality
store = StatisticsWrapper(
    LoggingWrapper(
        MemoryStore()
    )
)
```

### Execution Order

Wrappers execute in the order they are stacked:

- **Writes** (put, delete): Outer wrapper → Inner wrapper → Store
- **Reads** (get, ttl): Store → Inner wrapper → Outer wrapper

## Available Wrappers

### CompressionWrapper

Compresses values before storing and decompresses on retrieval using gzip
compression.

::: key_value.aio.wrappers.compression.CompressionWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Storing large JSON objects
- Reducing network transfer for distributed stores
- Optimizing disk usage

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.compression import CompressionWrapper

store = CompressionWrapper(
    key_value=MemoryStore(),
    min_size_to_compress=1024  # Only compress values > 1KB
)

# Large values are automatically compressed
await store.put(
    key="large-doc",
    value={"content": "..." * 1000},
    collection="documents"
)
```

#### Performance Considerations

- Compression adds CPU overhead but reduces storage/transfer size
- The `min_size_to_compress` parameter helps avoid compressing small values
  where overhead exceeds benefit
- Uses gzip with compression level 1 for speed

---

### FernetEncryptionWrapper

Encrypts values before storing and decrypts on retrieval using Fernet symmetric
encryption.

::: key_value.aio.wrappers.encryption.fernet.FernetEncryptionWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Storing sensitive data (passwords, tokens, PII)
- Compliance with data protection regulations
- Encrypting data at rest

#### Example

```python
from cryptography.fernet import Fernet
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption.fernet import FernetEncryptionWrapper

# Generate or load a key
key = Fernet.generate_key()
fernet = Fernet(key)

store = FernetEncryptionWrapper(
    key_value=MemoryStore(),
    fernet=fernet,
    raise_on_decryption_error=True
)

# Values are automatically encrypted
await store.put(
    key="secret",
    value={"password": "super-secret"},
    collection="credentials"
)
```

#### Security Considerations

- Store encryption keys securely (e.g., environment variables, key management
  services)
- Use `MultiFernet` for key rotation
- Set `raise_on_decryption_error=True` to detect tampering

---

### FallbackWrapper

Provides failover to a secondary store if the primary store fails.

::: key_value.aio.wrappers.fallback.FallbackWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- High availability setups
- Gradual migration between stores
- Local cache with remote fallback

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.stores.redis import RedisStore
from key_value.aio.wrappers.fallback import FallbackWrapper

store = FallbackWrapper(
    primary=RedisStore(url="redis://localhost:6379/0"),
    fallback=MemoryStore()
)

# If Redis is unavailable, operations fall back to MemoryStore
user = await store.get(key="user:123", collection="users")
```

---

### LimitSizeWrapper

Enforces size limits on stored values, raising an error if values exceed the
specified size.

::: key_value.aio.wrappers.limit_size.LimitSizeWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Preventing storage of excessively large values
- Enforcing data constraints
- Protecting against abuse

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.limit_size import LimitSizeWrapper

store = LimitSizeWrapper(
    key_value=MemoryStore(),
    max_size=10240  # 10KB limit
)

# Raises ValueError if value exceeds 10KB
await store.put(
    key="doc",
    value={"content": "..."},
    collection="documents"
)
```

---

### LoggingWrapper

Logs all key-value operations for debugging and auditing.

::: key_value.aio.wrappers.logging.LoggingWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Debugging store operations
- Auditing data access
- Performance monitoring

#### Example

```python
import logging
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.logging import LoggingWrapper

logging.basicConfig(level=logging.INFO)

store = LoggingWrapper(
    key_value=MemoryStore(),
    log_level=logging.INFO
)

# All operations are logged
await store.put(key="test", value={"data": "value"})
# INFO: PUT key='test' collection=None ttl=None
```

---

### PassthroughCacheWrapper

Provides read-through caching with a fast local cache and a slower remote store.

::: key_value.aio.wrappers.passthrough_cache.PassthroughCacheWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Reducing latency for frequently accessed data
- Reducing load on remote stores
- Hybrid local/remote architectures

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.stores.redis import RedisStore
from key_value.aio.wrappers.passthrough_cache import PassthroughCacheWrapper

store = PassthroughCacheWrapper(
    cache=MemoryStore(),  # Fast local cache
    store=RedisStore(url="redis://localhost:6379/0")  # Remote store
)

# First read: from Redis, cached in memory
user = await store.get(key="user:123", collection="users")

# Second read: from memory cache (faster)
user = await store.get(key="user:123", collection="users")
```

---

### PrefixCollectionsWrapper

Adds a prefix to all collection names.

::: key_value.aio.wrappers.prefix_collections.PrefixCollectionsWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Multi-tenant applications
- Environment separation (dev/staging/prod)
- Namespace isolation

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.prefix_collections import PrefixCollectionsWrapper

store = PrefixCollectionsWrapper(
    key_value=MemoryStore(),
    prefix="prod"
)

# Collection becomes "prod:users"
await store.put(
    key="alice",
    value={"name": "Alice"},
    collection="users"
)
```

---

### PrefixKeysWrapper

Adds a prefix to all keys.

::: key_value.aio.wrappers.prefix_keys.PrefixKeysWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Namespace isolation within collections
- Multi-tenant applications
- Avoiding key collisions

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.prefix_keys import PrefixKeysWrapper

store = PrefixKeysWrapper(
    key_value=MemoryStore(),
    prefix="app1"
)

# Key becomes "app1:user:123"
await store.put(
    key="user:123",
    value={"name": "Alice"},
    collection="users"
)
```

---

### ReadOnlyWrapper

Prevents all write operations, making the store read-only.

::: key_value.aio.wrappers.read_only.ReadOnlyWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Shared read-only caches
- Preventing accidental writes
- Read replicas

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.read_only import ReadOnlyWrapper

store = ReadOnlyWrapper(
    key_value=MemoryStore()
)

# Raises ReadOnlyError
await store.put(key="test", value={"data": "value"})
```

---

### RetryWrapper

Automatically retries failed operations with exponential backoff.

::: key_value.aio.wrappers.retry.RetryWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Handling transient network failures
- Improving reliability with remote stores
- Rate limit handling

#### Example

```python
from key_value.aio.stores.redis import RedisStore
from key_value.aio.wrappers.retry import RetryWrapper

store = RetryWrapper(
    key_value=RedisStore(url="redis://localhost:6379/0"),
    max_retries=3,
    initial_delay=0.1,
    max_delay=5.0,
    exponential_base=2
)

# Automatically retries on failure
user = await store.get(key="user:123", collection="users")
```

---

### SingleCollectionWrapper

Forces all operations to use a single collection, ignoring the collection
parameter.

::: key_value.aio.wrappers.single_collection.SingleCollectionWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Simplifying stores that don't need collections
- Migrating from non-collection-based stores
- Enforcing single-collection usage

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.single_collection import SingleCollectionWrapper

store = SingleCollectionWrapper(
    key_value=MemoryStore(),
    collection="default"
)

# All operations use "default" collection regardless of parameter
await store.put(key="test", value={"data": "value"}, collection="ignored")
```

---

### TTLClampWrapper

Clamps TTL values to a specified range, ensuring TTLs are within acceptable
bounds.

::: key_value.aio.wrappers.ttl_clamp.TTLClampWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Enforcing minimum/maximum TTLs
- Preventing excessively long or short TTLs
- Backend-specific TTL limitations

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.ttl_clamp import TTLClampWrapper

store = TTLClampWrapper(
    key_value=MemoryStore(),
    min_ttl=60,      # Minimum 1 minute
    max_ttl=86400    # Maximum 1 day
)

# TTL is clamped to range [60, 86400]
await store.put(
    key="test",
    value={"data": "value"},
    ttl=30  # Clamped to 60
)
```

---

### StatisticsWrapper

Tracks operation statistics including counts, hits, and misses.

::: key_value.aio.wrappers.statistics.StatisticsWrapper
    options:
      show_source: false
      members:
        - **init**
        - get_statistics
        - reset_statistics

#### Use Cases

- Performance monitoring
- Cache hit rate analysis
- Usage analytics

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.statistics import StatisticsWrapper

store = StatisticsWrapper(
    key_value=MemoryStore()
)

# Perform operations
await store.put(key="user:1", value={"name": "Alice"})
await store.get(key="user:1")  # Hit
await store.get(key="user:2")  # Miss

# Check statistics
stats = store.get_statistics()
print(f"Get operations: {stats.get.count}")
print(f"Get hits: {stats.get.hits}")
print(f"Get misses: {stats.get.misses}")
print(f"Hit rate: {stats.get.hit_rate()}")

# Reset statistics
store.reset_statistics()
```

---

### TimeoutWrapper

Adds timeout constraints to all operations, raising an error if operations
exceed the specified timeout.

::: key_value.aio.wrappers.timeout.TimeoutWrapper
    options:
      show_source: false
      members:
        - **init**

#### Use Cases

- Preventing operations from hanging indefinitely
- Enforcing SLA requirements
- Circuit breaker patterns

#### Example

```python
from key_value.aio.stores.redis import RedisStore
from key_value.aio.wrappers.timeout import TimeoutWrapper

store = TimeoutWrapper(
    key_value=RedisStore(url="redis://localhost:6379/0"),
    timeout=1.0  # 1 second timeout
)

# Raises asyncio.TimeoutError if operation takes > 1 second
user = await store.get(key="user:123", collection="users")
```

---

## Wrapper Stacking Guide

Wrappers can be stacked in any order, but some orderings are more effective than
others. Here are some recommended patterns:

### Performance Monitoring

```python
StatisticsWrapper(
    LoggingWrapper(
        TimeoutWrapper(
            store
        )
    )
)
```

### Production Ready

```python
StatisticsWrapper(
    RetryWrapper(
        TimeoutWrapper(
            CompressionWrapper(
                FernetEncryptionWrapper(
                    store
                )
            )
        )
    )
)
```

### Development

```python
LoggingWrapper(
    StatisticsWrapper(
        store
    )
)
```

### Multi-Tenant

```python
PrefixCollectionsWrapper(
    PrefixKeysWrapper(
        store
    )
)
```

## Creating Custom Wrappers

To create a custom wrapper, extend `BaseWrapper` and override the methods you
want to modify:

```python
from key_value.aio.wrappers.base import BaseWrapper
from typing_extensions import override

class CustomWrapper(BaseWrapper):
    def __init__(self, key_value: AsyncKeyValue):
        self.key_value = key_value
        super().__init__()

    @override
    async def get(self, key: str, *, collection: str | None = None):
        # Add custom logic before
        print(f"Getting key: {key}")

        # Call wrapped store
        result = await self.key_value.get(key=key, collection=collection)

        # Add custom logic after
        print(f"Got result: {result}")

        return result
```

See the [API Reference](api/wrappers.md) for complete wrapper documentation.
