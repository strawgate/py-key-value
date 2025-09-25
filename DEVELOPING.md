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
├── stores/                     # Store implementations
│   ├── base/                   # Abstract base classes
│   ├── redis/                  # Redis implementation
│   ├── memory/                 # In-memory TLRU cache
│   ├── disk/                   # Disk-based storage
│   ├── elasticsearch/          # Elasticsearch implementation
│   ├── simple/                 # Simple dict-based stores
│   ├── null/                   # Null object pattern store
│   ├── utils/                  # Utility functions
│   │   ├── compound_keys.py    # Key composition utilities
│   │   ├── managed_entry.py    # ManagedEntry dataclass
│   │   └── time_to_live.py     # TTL calculation
│   └── wrappers/               # Wrappers implementations

tests/
├── conftest.py                 # Test configuration
├── cases.py                    # Common test cases
├── test_types.py              # Type tests
└── stores/                     # Store-specific tests
```

## Store Configuration

All stores implement the `KVStoreProtocol` interface. Here are detailed configuration options:

### Redis Store
High-performance store with native TTL support:

```python
from kv_store_adapter import RedisStore

# Connection options
store = RedisStore(host="localhost", port=6379, db=0, password="secret")
store = RedisStore(url="redis://localhost:6379/0")
store = RedisStore(client=existing_redis_client)
```

### DynamoDB Store
AWS DynamoDB-based store with native TTL and auto-scaling:

```python
from kv_store_adapter import DynamoDBStore

# Using AWS credentials (IAM role, environment variables, or AWS CLI)
store = DynamoDBStore(table_name="my-kv-table", region_name="us-east-1")

# Using explicit credentials
store = DynamoDBStore(
    table_name="my-kv-table",
    region_name="us-west-2",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key"
)

# Using existing boto3 client
store = DynamoDBStore(table_name="my-kv-table", client=existing_dynamodb_client)
```

### Memory Store
In-memory TLRU (Time-aware Least Recently Used) cache:

```python
from kv_store_adapter import MemoryStore

store = MemoryStore(max_entries=1000)  # Default: 1000 entries
```

### Disk Store
Persistent disk-based storage using diskcache:

```python
from kv_store_adapter import DiskStore

store = DiskStore(path="/path/to/cache", size_limit=1024*1024*1024)  # 1GB
store = DiskStore(cache=existing_cache_instance)
```

### Elasticsearch Store
Full-text searchable storage with Elasticsearch:

```python
from kv_store_adapter import ElasticsearchStore

store = ElasticsearchStore(
    url="https://localhost:9200",
    api_key="your-api-key",
    index="kv-store"
)
store = ElasticsearchStore(client=existing_client, index="custom-index")
```

### Simple Stores
Dictionary-based stores for testing and development:

```python
from kv_store_adapter import SimpleStore, SimpleManagedStore, SimpleJSONStore

# Basic dictionary store
store = SimpleStore(max_entries=1000)

# Managed store with automatic entry wrapping  
managed_store = SimpleManagedStore(max_entries=1000)

# JSON-serialized storage
json_store = SimpleJSONStore(max_entries=1000)
```

### Null Store
Null object pattern store for testing:

```python
from kv_store_adapter import NullStore

store = NullStore()  # Accepts all operations but stores nothing
```

## Architecture

### Store Types

The project supports two main store architectures:

1. **Unmanaged Stores (`BaseKVStore`)**
   - Handle their own TTL management
   - Directly store user values
   - Examples: `SimpleStore`, `NullStore`

2. **Managed Stores (`BaseManagedKVStore`)**
   - Use `ManagedEntry` wrapper objects
   - Automatic TTL handling and expiration checking
   - Examples: `RedisStore`, `MemoryStore`, `DiskStore`, `ElasticsearchStore`

### Key Concepts

- **Collections**: Logical namespaces for organizing keys
- **Compound Keys**: Internal key format `collection::key` for flat stores
- **TTL Management**: Automatic expiration handling with timezone-aware timestamps
- **Wrappers**: Wrapper pattern for adding functionality (statistics, logging, etc.)

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
from kv_store_adapter.stores.mystore import MyStore
from tests.cases import BaseKVStoreTestCase

class TestMyStore(BaseKVStoreTestCase):
    @pytest.fixture
    async def store(self):
        """Provide store instance for testing."""
        store = MyStore()
        yield store
        # Cleanup if needed
        await store.clear_collection("test")
```

#### Common Test Cases

Use the provided base test cases for consistency:

```python
from tests.cases import BaseKVStoreTestCase, BaseManagedKVStoreTestCase

class TestMyUnmanagedStore(BaseKVStoreTestCase):
    # Inherits all standard KV store tests
    pass

class TestMyManagedStore(BaseManagedKVStoreTestCase):
    # Inherits managed store specific tests
    pass
```

#### Custom Test Methods

Add store-specific tests as needed:

```python
class TestRedisStore(BaseManagedKVStoreTestCase):
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

Decide between `BaseKVStore` (unmanaged) or `BaseManagedKVStore` (managed):

```python
from kv_store_adapter.stores.base.unmanaged import BaseKVStore
# or
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
```

### 2. Create Store Class

```python
# src/kv_store_adapter/stores/mystore/store.py
from typing import Any
from kv_store_adapter.stores.base.managed import BaseManagedKVStore
from kv_store_adapter.stores.utils.managed_entry import ManagedEntry

class MyStore(BaseManagedKVStore):
    """My custom key-value store implementation."""
    
    def __init__(self, **kwargs):
        """Initialize store with custom parameters."""
        super().__init__()
        # Your initialization code
    
    async def setup(self) -> None:
        """Initialize store (called once before first use)."""
        # Setup code (connect to database, etc.)
        pass
    
    async def get_entry(self, collection: str, key: str) -> ManagedEntry | None:
        """Retrieve a managed entry by key from the specified collection."""
        # Your implementation
        pass
    
    async def put_entry(
        self,
        collection: str,
        key: str,
        cache_entry: ManagedEntry,
        *,
        ttl: float | None = None
    ) -> None:
        """Store a managed entry by key in the specified collection."""
        # Your implementation
        pass
    
    # Implement other required methods...
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
from kv_store_adapter.stores.mystore import MyStore
from tests.cases import BaseManagedKVStoreTestCase

class TestMyStore(BaseManagedKVStoreTestCase):
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
