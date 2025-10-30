# Stores

Stores are the core implementations of the `AsyncKeyValue` protocol. They provide
the actual storage backend for your key-value data.

## Store Categories

Stores are organized into three categories based on their storage location and
use case:

- **Local Stores**: In-memory or on-disk storage local to the application
- **Secret Stores**: Secure storage for sensitive data in OS secret stores
- **Distributed Stores**: Network-based storage for multi-node applications

## Stability Levels

Each store has a stability rating that indicates the likelihood of
backwards-incompatible changes to how data is stored:

- **Stable**: No planned backwards-incompatible changes
- **Preview**: Unlikely to change in backwards-incompatible ways
- **Unstable**: May change in backwards-incompatible ways
- **N/A**: Not applicable (e.g., in-memory stores)

If you're using py-key-value for caching, stability may not matter. For
long-term storage, prefer stable stores.

## Local Stores

Local stores are stored in memory or on disk, local to the application.

| Store | Stability | Async | Sync | Description |
|-------|:---------:|:-----:|:----:|:------------|
| Memory | N/A | ✅ | ✅ | Fast in-memory storage for development and caching |
| Disk | Stable | ☑️ | ✅ | Persistent file-based storage in a single file |
| Disk (Per-Collection) | Stable | ☑️ | ✅ | Persistent storage with separate files per collection |
| Null (test) | N/A | ✅ | ✅ | No-op store for testing without side effects |
| RocksDB | Unstable | ☑️ | ✅ | High-performance embedded database |
| Simple (test) | N/A | ✅ | ✅ | Simple in-memory store for testing |
| Windows Registry | Unstable | ☑️ | ✅ | Windows Registry-based storage |

**Legend:**

- ✅ = Fully async implementation available
- ☑️ = Available but uses synchronous underlying implementation
- ✖️ = Not available

### MemoryStore

Fast in-memory storage ideal for development, testing, and caching.

```python
from key_value.aio.stores.memory import MemoryStore

store = MemoryStore()
```

**Installation:**

```bash
pip install py-key-value-aio[memory]
```

**Use Cases:**

- Development and testing
- Fast caching
- Session storage
- Temporary data

**Characteristics:**

- No persistence (data lost on restart)
- Extremely fast
- No external dependencies
- Thread-safe

---

### DiskStore

Persistent file-based storage using a single JSON file.

```python
from key_value.aio.stores.disk import DiskStore

store = DiskStore(directory="./cache")
```

**Installation:**

```bash
pip install py-key-value-aio[disk]
```

**Use Cases:**

- Local caching with persistence
- Development environments
- Single-node applications
- Small datasets

**Characteristics:**

- Persists across restarts
- Simple file format (JSON)
- Suitable for small to medium datasets
- All data in one file

---

### MultiDiskStore

Persistent storage with separate files per collection.

```python
from key_value.aio.stores.multi_disk import MultiDiskStore

store = MultiDiskStore(directory="./cache")
```

**Installation:**

```bash
pip install py-key-value-aio[disk]
```

**Use Cases:**

- Organizing data by collection
- Better performance with many collections
- Easier to manage individual collections

**Characteristics:**

- One file per collection
- Better suited for many collections
- Easier collection management
- JSON-based storage

---

### RocksDBStore

High-performance embedded database using RocksDB.

```python
from key_value.aio.stores.rocksdb import RocksDBStore

store = RocksDBStore(path="./rocksdb")
```

**Installation:**

```bash
pip install py-key-value-aio[rocksdb]
```

**Use Cases:**

- High-throughput applications
- Large datasets
- Performance-critical applications

**Characteristics:**

- Very fast reads and writes
- Efficient storage
- Requires native dependencies
- Stable storage format: **Unstable**

---

### WindowsRegistryStore

Storage using the Windows Registry.

```python
from key_value.aio.stores.registry import WindowsRegistryStore

store = WindowsRegistryStore(
    hive="HKEY_CURRENT_USER",
    registry_path="Software\\py-key-value"
)
```

**Installation:**

```bash
pip install py-key-value-aio[registry]
```

**Use Cases:**

- Windows-specific applications
- System configuration storage
- Integration with Windows settings

**Characteristics:**

- Windows-only
- Persists in registry
- Subject to registry limitations
- Stable storage format: **Unstable**

---

### NullStore

No-op store that discards all data. Useful for testing.

```python
from key_value.aio.stores.null import NullStore

store = NullStore()
```

**Use Cases:**

- Testing without side effects
- Disabling storage temporarily
- Performance baseline testing

---

### SimpleStore

Simple in-memory store for testing.

```python
from key_value.aio.stores.simple import SimpleStore

store = SimpleStore()
```

**Use Cases:**

- Basic testing
- Minimal implementation reference

---

## Secret Stores

Secret stores provide secure storage for sensitive data, typically using
operating system secret management facilities.

| Store | Stability | Async | Sync | Description |
|-------|:---------:|:-----:|:----:|:------------|
| Keyring | Stable | ✅ | ✅ | OS-level secure storage (Keychain, Credential Manager, etc.) |
| Vault | Unstable | ✅ | ✅ | HashiCorp Vault integration for enterprise secrets |

### KeyringStore

Secure storage using the operating system's keyring (macOS Keychain, Windows
Credential Manager, Linux Secret Service).

```python
from key_value.aio.stores.keyring import KeyringStore

store = KeyringStore(service_name="py-key-value")
```

**Installation:**

```bash
pip install py-key-value-aio[keyring]
```

**Use Cases:**

- Storing API keys and tokens
- User credentials
- Sensitive configuration
- Encrypted local storage

**Characteristics:**

- OS-level encryption
- Secure by default
- Cross-platform
- **Windows limitation**: Strict value length limits

**Important:** Windows Keyring has strict limits on value length which may
cause issues with large values.

---

### VaultStore

Integration with HashiCorp Vault for enterprise secret management.

```python
from key_value.aio.stores.vault import VaultStore

store = VaultStore(
    url="http://localhost:8200",
    token="your-token"
)
```

**Installation:**

```bash
pip install py-key-value-aio[vault]
```

**Use Cases:**

- Enterprise secret management
- Multi-environment deployments
- Centralized secret rotation
- Audit logging

**Characteristics:**

- Enterprise-grade security
- Centralized management
- Audit logging
- Stable storage format: **Unstable**

---

## Distributed Stores

Distributed stores provide network-based storage for multi-node applications.

| Store | Stability | Async | Sync | Description |
|-------|:---------:|:-----:|:----:|:------------|
| DynamoDB | Unstable | ✅ | ✖️ | AWS DynamoDB key-value storage |
| Elasticsearch | Unstable | ✅ | ✅ | Full-text search with key-value capabilities |
| Memcached | Unstable | ✅ | ✖️ | High-performance distributed memory cache |
| MongoDB | Unstable | ✅ | ✅ | Document database used as key-value store |
| PostgreSQL | Unstable | ✅ | ✖️ | PostgreSQL-based key-value storage |
| Redis | Stable | ✅ | ✅ | Popular in-memory data structure store |
| Valkey | Stable | ✅ | ✅ | Open-source Redis fork |

### RedisStore

High-performance in-memory store using Redis.

```python
from key_value.aio.stores.redis import RedisStore

store = RedisStore(url="redis://localhost:6379/0")
```

**Installation:**

```bash
pip install py-key-value-aio[redis]
```

**Use Cases:**

- Distributed caching
- Session storage
- Real-time applications
- High-throughput systems

**Characteristics:**

- Very fast (in-memory)
- Production-ready
- Rich feature set
- Horizontal scaling support
- **Stable storage format**

---

### ValkeyStore

Open-source Redis fork with similar performance characteristics.

```python
from key_value.aio.stores.valkey import ValkeyStore

store = ValkeyStore(host="localhost", port=6379)
```

**Installation:**

```bash
pip install py-key-value-aio[valkey]
```

**Use Cases:**

- Same as Redis
- Open-source preference
- Redis API compatibility

**Characteristics:**

- Redis-compatible
- Open-source governance
- Production-ready
- **Stable storage format**

---

### DynamoDBStore

AWS DynamoDB integration for serverless and cloud-native applications.

```python
from key_value.aio.stores.dynamodb import DynamoDBStore

store = DynamoDBStore(
    table_name="kv-store",
    region_name="us-east-1"
)
```

**Installation:**

```bash
pip install py-key-value-aio[dynamodb]
```

**Use Cases:**

- AWS-native applications
- Serverless architectures
- Global distribution
- Managed infrastructure

**Characteristics:**

- Fully managed
- Auto-scaling
- Global tables
- Pay-per-use pricing
- Stable storage format: **Unstable**

---

### ElasticsearchStore

Full-text search engine used as a key-value store.

```python
from key_value.aio.stores.elasticsearch import ElasticsearchStore

store = ElasticsearchStore(
    url="https://localhost:9200",
    api_key="your-api-key",
    index="kv-store"
)
```

**Installation:**

```bash
pip install py-key-value-aio[elasticsearch]
```

**Use Cases:**

- Applications already using Elasticsearch
- Need for search capabilities
- Analytics and logging

**Characteristics:**

- Search capabilities
- Distributed by default
- Rich querying
- Stable storage format: **Unstable**

---

### MongoDBStore

Document database used as a key-value store.

```python
from key_value.aio.stores.mongodb import MongoDBStore

store = MongoDBStore(url="mongodb://localhost:27017/test")
```

**Installation:**

```bash
pip install py-key-value-aio[mongodb]
```

**Use Cases:**

- Applications already using MongoDB
- Document-oriented data
- Flexible schemas

**Characteristics:**

- Document storage
- Rich querying
- Horizontal scaling
- Stable storage format: **Unstable**

---

### PostgreSQLStore

PostgreSQL relational database used as a key-value store.

```python
from key_value.aio.stores.postgresql import PostgreSQLStore

# Using connection URL
store = PostgreSQLStore(url="postgresql://localhost:5432/mydb")

# Using connection parameters
store = PostgreSQLStore(
    host="localhost",
    port=5432,
    database="mydb",
    user="myuser",
    password="mypass"
)
```

**Installation:**

```bash
pip install py-key-value-aio[postgresql]
```

**Use Cases:**

- Applications already using PostgreSQL
- Need for SQL query capabilities
- ACID compliance requirements
- Relational data with key-value access pattern

**Characteristics:**

- JSONB storage for values
- TTL support via expires_at timestamps
- Optimized bulk operations
- Lazy cleanup of expired entries
- Stable storage format: **Unstable**

---

### MemcachedStore

High-performance distributed memory caching system.

```python
from key_value.aio.stores.memcached import MemcachedStore

store = MemcachedStore(host="127.0.0.1", port=11211)
```

**Installation:**

```bash
pip install py-key-value-aio[memcached]
```

**Use Cases:**

- Distributed caching
- Session storage
- High-throughput caching

**Characteristics:**

- Very fast
- Simple protocol
- Distributed by design
- No persistence
- Stable storage format: **Unstable**

---

## Choosing a Store

### Development

**Recommended:** `MemoryStore` or `DiskStore`

- Fast iteration
- No setup required
- Easy debugging

```python
# Development
from key_value.aio.stores.memory import MemoryStore
store = MemoryStore()
```

### Production Caching

**Recommended:** `RedisStore` or `ValkeyStore`

- High performance
- Distributed
- Production-ready
- Stable storage format

```python
# Production caching
from key_value.aio.stores.redis import RedisStore
store = RedisStore(url="redis://localhost:6379/0")
```

### Long-Term Storage

**Recommended:** Stores with **Stable** stability rating

- `RedisStore`
- `ValkeyStore`
- `DiskStore`
- `MultiDiskStore`
- `KeyringStore`

Avoid unstable stores for data you can't afford to lose or migrate.

### Sensitive Data

**Recommended:** `KeyringStore` or `VaultStore`

- OS-level encryption
- Secure by default
- Audit logging (Vault)

```python
# Sensitive data
from key_value.aio.stores.keyring import KeyringStore
store = KeyringStore(service_name="my-app")
```

### Serverless/Cloud

**Recommended:** `DynamoDBStore` (AWS)

- Fully managed
- Auto-scaling
- No servers to maintain

```python
# AWS Lambda
from key_value.aio.stores.dynamodb import DynamoDBStore
store = DynamoDBStore(table_name="kv-store", region_name="us-east-1")
```

## Store Compatibility

All stores implement the same protocol, making it easy to switch:

```python
# Development
store = MemoryStore()

# Production
store = RedisStore(url="redis://localhost:6379/0")

# Your code works with both!
await store.put(key="user:123", value={"name": "Alice"}, collection="users")
```

See the [API Reference](api/stores.md) for complete store documentation.
