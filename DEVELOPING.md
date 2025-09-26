# Development Guide

This guide covers development setup, testing, and contribution guidelines for the KV Store Adapter project.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) for dependency management
- Docker and Docker Compose (for integration tests)

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd py-kv-store-adapter
   ```

2. **Install dependencies:**
   ```bash
   uv sync --group dev
   ```

3. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate  # Linux/macOS
   # or
   .venv\Scripts\activate  # Windows
   ```

4. **Install pre-commit hooks (optional but recommended):**
   ```bash
   pre-commit install
   ```

## Project Structure

```
src/kv_store_adapter/
├── __init__.py                 # Main package exports
├── types.py                    # Core types and protocols
├── errors.py                   # Exception hierarchy
├── adapters/                   # Protocol adapters
│   ├── __init__.py            # Adapter exports  
│   ├── pydantic.py            # Pydantic model adapter
│   └── raise_on_missing.py    # Raise-on-missing adapter
├── stores/                     # Store implementations
│   ├── __init__.py            # Store exports
│   ├── base.py                # Abstract base classes
│   ├── redis/                 # Redis implementation
│   │   ├── __init__.py        # Redis exports
│   │   └── store.py           # RedisStore implementation
│   ├── memory/                # In-memory TLRU cache
│   │   ├── __init__.py        # Memory exports
│   │   └── store.py           # MemoryStore implementation
│   ├── disk/                  # Disk-based storage
│   │   ├── __init__.py        # Disk exports
│   │   ├── store.py           # DiskStore implementation
│   │   └── multi_store.py     # Multi-disk store
│   ├── elasticsearch/         # Elasticsearch implementation
│   │   ├── __init__.py        # Elasticsearch exports
│   │   ├── store.py           # ElasticsearchStore implementation
│   │   └── utils.py           # Elasticsearch utilities
│   ├── simple/                # Simple dict-based stores
│   │   ├── __init__.py        # Simple store exports
│   │   └── store.py           # SimpleStore implementation
│   ├── null/                  # Null object pattern store
│   │   ├── __init__.py        # Null store exports
│   │   └── store.py           # NullStore implementation
│   └── utils/                 # Utility functions
│       ├── compound.py        # Key composition utilities
│       ├── managed_entry.py   # ManagedEntry dataclass
│       └── time_to_live.py    # TTL calculation
├── wrappers/                  # Wrapper implementations
│   ├── __init__.py            # Wrapper exports
│   ├── base.py                # Base wrapper class
│   ├── statistics.py          # Statistics tracking wrapper
│   ├── clamp_ttl.py           # TTL clamping wrapper
│   ├── passthrough_cache.py   # Passthrough cache wrapper
│   ├── prefix_collections.py  # Collection prefix wrapper
│   ├── prefix_keys.py         # Key prefix wrapper
│   └── single_collection.py   # Single collection wrapper

tests/
├── conftest.py                 # Test configuration
├── cases.py                    # Common test cases
├── test_types.py              # Type tests
└── stores/                     # Store-specific tests
```

## Store Configuration

All stores implement the `KVStore` interface. Here are detailed configuration options:

### Redis Store
High-performance store with native TTL support:

```python
from kv_store_adapter.stores.redis.store import RedisStore

# Connection options
store = RedisStore(host="localhost", port=6379, db=0, password="secret")
store = RedisStore(url="redis://localhost:6379/0")
store = RedisStore(client=existing_redis_client)
```

### Memory Store
In-memory TLRU (Time-aware Least Recently Used) cache:

```python
from kv_store_adapter.stores.memory.store import MemoryStore

store = MemoryStore(max_entries_per_collection=1000)  # Default: 1000 entries per collection
```

### Disk Store
Persistent disk-based storage using diskcache:

```python
from kv_store_adapter.stores.disk.store import DiskStore

store = DiskStore(directory="/path/to/cache", size_limit=1024*1024*1024)  # 1GB
store = DiskStore(disk_cache=existing_cache_instance)
```

### Elasticsearch Store
Full-text searchable storage with Elasticsearch:

```python
from kv_store_adapter.stores.elasticsearch.store import ElasticsearchStore

store = ElasticsearchStore(
    url="https://localhost:9200",
    api_key="your-api-key",
    index="kv-store"
)
store = ElasticsearchStore(client=existing_client, index="custom-index")
```

### Simple Store
Dictionary-based store for testing and development:

```python
from kv_store_adapter.stores.simple.store import SimpleStore

# Basic managed dictionary store
store = SimpleStore(max_entries=1000)
```

### Null Store
Null object pattern store for testing:

```python
from kv_store_adapter.stores.null.store import NullStore

store = NullStore()  # Accepts all operations but stores nothing
```

## Architecture

### Store Types

All stores now inherit from the unified `BaseStore` class which uses `ManagedEntry` objects:

1. **Managed Stores (`BaseStore`)**
   - Use `ManagedEntry` wrapper objects for consistent TTL and metadata handling
   - Automatic TTL handling and expiration checking
   - Consistent behavior across all store implementations
   - Examples: `RedisStore`, `MemoryStore`, `DiskStore`, `ElasticsearchStore`, `SimpleStore`, `NullStore`

### Key Concepts

- **Collections**: Logical namespaces for organizing keys
- **Compound Keys**: Internal key format `collection::key` for flat stores
- **TTL Management**: Automatic expiration handling with timezone-aware timestamps
- **Wrappers**: Wrapper pattern for adding functionality (statistics, TTL clamping, prefixing, etc.)
- **Adapters**: Transform data to/from stores (Pydantic models, raise-on-missing behavior, etc.)

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src/kv_store_adapter --cov-report=html

# Run specific test file
uv run pytest tests/stores/redis/test_redis.py

# Run tests with specific markers
uv run pytest -m "not skip_on_ci"
```

### Test Environment Setup

Some tests require external services. Use Docker Compose to start them:

```bash
# Start all services
docker-compose up -d

# Start specific services
docker-compose up -d redis elasticsearch

# Stop services
docker-compose down
```

### Environment Variables

Create a `.env` file for test configuration:

```bash
# Redis
REDIS_URL=redis://localhost:6379/0

# Elasticsearch
ELASTICSEARCH_URL=https://localhost:9200
ELASTICSEARCH_API_KEY=your-api-key-here
ELASTICSEARCH_INDEX=test-kv-store

# Test settings
SKIP_INTEGRATION_TESTS=false
```

### Writing Tests

#### Test Structure

Tests are organized by store type and use common test cases:

```python
# tests/stores/mystore/test_mystore.py
import pytest
from kv_store_adapter.stores.mystore.store import MyStore
from tests.stores.conftest import BaseStoreTests

class TestMyStore(BaseStoreTests):
    @pytest.fixture
    async def store(self):
        """Provide store instance for testing."""
        store = MyStore()
        yield store
        # Cleanup if needed
        await store.destroy()
```

#### Common Test Cases

Use the provided base test cases for consistency:

```python
from tests.stores.conftest import BaseStoreTests

class TestMyStore(BaseStoreTests):
    # Inherits all standard KV store tests
    pass
```

#### Custom Test Methods

Add store-specific tests as needed:

```python
class TestRedisStore(BaseStoreTests):
    async def test_redis_specific_feature(self, store):
        """Test Redis-specific functionality."""
        # Your test implementation
        pass
```

### Test Markers

- `skip_on_ci`: Skip tests that require external services on CI
- `slow`: Mark slow-running tests
- `integration`: Mark integration tests

## Code Quality

### Linting and Formatting

The project uses Ruff for linting and formatting:

```bash
# Check code style
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix

# Format code
uv run ruff format
```

### Type Checking

Use Pyright for type checking:

```bash
# Check types
pyright

# Check specific file
pyright src/kv_store_adapter/stores/redis/store.py
```

## Adding New Store Implementations

### 1. Choose Base Class

All stores inherit from the unified `BaseStore` class, which provides consistent TTL and metadata handling:

```python
from kv_store_adapter.stores.base import BaseStore
```

You can also inherit from specialized base classes for additional functionality:
- `BaseEnumerateKeysStore` - Adds key enumeration support
- `BaseEnumerateCollectionsStore` - Adds collection enumeration support  
- `BaseDestroyStore` - Adds store destruction support
- `BaseDestroyCollectionStore` - Adds collection destruction support
- `BaseCullStore` - Adds expired entry culling support

### 2. Create Store Class

```python
# src/kv_store_adapter/stores/mystore/store.py
from typing_extensions import override
from kv_store_adapter.stores.base import BaseStore
from kv_store_adapter.utils.managed_entry import ManagedEntry

class MyStore(BaseStore):
    """My custom key-value store implementation."""
    
    def __init__(self, *, default_collection: str | None = None, **kwargs):
        """Initialize store with custom parameters."""
        super().__init__(default_collection=default_collection)
        # Your initialization code
    
    async def _setup(self) -> None:
        """Initialize store (called once before first use)."""
        # Setup code (connect to database, etc.)
        pass
    
    @override
    async def _get_managed_entry(self, *, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection.
        
        Returns:
            ManagedEntry if found, None if not found or expired.
        """
        # Your implementation
        pass
    
    @override
    async def _put_managed_entry(
        self,
        *,
        collection: str,
        key: str,
        managed_entry: ManagedEntry,
    ) -> None:
        """Store a managed entry by key in the specified collection.
        
        Args:
            collection: The collection to store in.
            key: The key to store under.
            managed_entry: The ManagedEntry containing value and metadata.
        """
        # Your implementation
        pass
    
    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        """Delete a managed entry by key from the specified collection.
        
        Args:
            key: The key to delete.
            collection: The collection to delete from.
            
        Returns:
            True if the key was deleted, False if it didn't exist.
        """
        # Your implementation
        pass
    
    # Implement other optional methods as needed...
```

### 3. Create Package Structure

```
src/kv_store_adapter/stores/mystore/
├── __init__.py          # Export store class
└── store.py            # Store implementation
```

```python
# src/kv_store_adapter/stores/mystore/__init__.py
from .store import MyStore

__all__ = ["MyStore"]
```

### 4. Add Tests

```python
# tests/stores/mystore/test_mystore.py
import pytest
from kv_store_adapter.stores.mystore.store import MyStore
from tests.stores.conftest import BaseStoreTests

class TestMyStore(BaseStoreTests):
    @pytest.fixture
    async def store(self):
        store = MyStore()
        yield store
        # Cleanup
```

### 5. Add Optional Dependencies

```toml
# pyproject.toml
[project.optional-dependencies]
mystore = ["my-store-dependency>=1.0.0"]
```
