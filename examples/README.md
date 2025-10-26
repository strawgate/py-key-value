# py-key-value Examples

This directory contains example projects demonstrating real-world use cases of
py-key-value with the PydanticAdapter and various wrappers.

All examples are **async-only** and include comprehensive test suites.

## Available Examples

| Example | Description | Key Features |
|---------|-------------|--------------|
| [chat_app](chat_app/) | Simple chat message storage | PydanticAdapter, StatisticsWrapper, TTLClampWrapper, LoggingWrapper |
| [trading_data](trading_data/) | Trading data cache with compression | PydanticAdapter, CompressionWrapper, PassthroughCacheWrapper, RetryWrapper |
| [web_scraper_cache](web_scraper_cache/) | Web scraper cache with encryption | PydanticAdapter, FernetEncryptionWrapper, LimitSizeWrapper, FallbackWrapper |

## Quick Start

Each example is a self-contained project with its own dependencies and tests.

### Installation

```bash
# Navigate to an example directory
cd chat_app

# Install dependencies
pip install -r requirements.txt

# Or using uv
uv pip install -e .
```

### Running Examples

```bash
# Run the example application
python chat_app.py  # or trading_app.py, scraper.py

# Run tests
pytest test_chat_app.py -v
```

## Example Overview

### 1. Chat App

**Use Case**: Simple chat message storage with automatic expiration

**Demonstrates**:
- Type-safe message storage with PydanticAdapter
- Automatic message expiration using TTLClampWrapper
- Operation statistics tracking with StatisticsWrapper
- Debug logging with LoggingWrapper

**Complexity**: Simple (50-75 lines)

**Best for**: Learning the basics of py-key-value and wrapper composition

[View Example →](chat_app/)

---

### 2. Trading Data Cache

**Use Case**: Cache stock/crypto price data with compression and multi-tier
caching

**Demonstrates**:
- Multi-tier caching (memory + disk) with PassthroughCacheWrapper
- Data compression with CompressionWrapper
- Automatic retry with RetryWrapper
- Cache hit/miss metrics with StatisticsWrapper

**Complexity**: Medium (100-125 lines)

**Best for**: Understanding advanced caching patterns and performance
optimization

[View Example →](trading_data/)

---

### 3. Web Scraper Cache

**Use Case**: Cache scraped web pages with encryption and size limits

**Demonstrates**:
- Encrypted storage with FernetEncryptionWrapper
- Size limits with LimitSizeWrapper (reject pages >5MB)
- TTL enforcement with TTLClampWrapper
- Fallback resilience with FallbackWrapper

**Complexity**: Medium (100-125 lines)

**Best for**: Learning security patterns and resilience strategies

[View Example →](web_scraper_cache/)

## Common Patterns

### PydanticAdapter

All examples use PydanticAdapter for type-safe storage:

```python
from pydantic import BaseModel
from key_value.aio.adapters.pydantic import PydanticAdapter

class MyModel(BaseModel):
    field1: str
    field2: int

adapter = PydanticAdapter[MyModel](
    key_value=store,
    pydantic_model=MyModel,
)

# Type-safe operations
await adapter.put(collection="my_collection", key="key1", value=MyModel(...))
model = await adapter.get(collection="my_collection", key="key1")  # Returns MyModel | None
```

### Wrapper Composition

Wrappers are composed inside-out, creating a processing pipeline:

```python
# Wrappers are applied from inside-out
wrapped_store = OuterWrapper(
    key_value=MiddleWrapper(
        key_value=InnerWrapper(
            key_value=BaseStore()
        )
    )
)

# Request flow: Outer → Middle → Inner → Base
```

### Collection-based Storage

Collections provide namespace isolation:

```python
# Different collections, same key - no collision
await adapter.put(collection="users", key="123", value=user)
await adapter.put(collection="orders", key="123", value=order)
```

## Testing

All examples include comprehensive test suites using pytest and pytest-asyncio.

### Running Tests

```bash
# Run tests for a specific example
cd chat_app
pytest test_chat_app.py -v

# Run all tests
pytest examples/*/test_*.py -v

# Run with coverage
pytest examples/*/test_*.py --cov
```

### Test Fixtures

Examples use pytest fixtures for setup and teardown:

```python
@pytest.fixture
async def cache(self, tmp_path) -> MyCache:
    """Create a cache instance for testing."""
    cache = MyCache(cache_dir=str(tmp_path / "test_cache"))
    yield cache
    await cache.cleanup()
```

## Requirements

- Python 3.10 or newer
- py-key-value-aio
- pydantic

Additional dependencies per example:
- **trading_data**: None (uses built-in stores)
- **web_scraper_cache**: cryptography (for encryption)

## Development

### Project Structure

Each example follows this structure:

```
example_name/
├── README.md           # Detailed documentation
├── pyproject.toml      # Project metadata and dependencies
├── requirements.txt    # Pip-compatible dependencies
├── __init__.py         # Package initialization
├── example_name.py     # Main example code
└── test_example.py     # Comprehensive test suite
```

### Adding New Examples

To add a new example:

1. Create a new directory under `examples/`
2. Follow the structure above
3. Include comprehensive README with:
   - Overview and use case
   - Features demonstrated
   - Installation and usage instructions
   - Key concepts explained
   - Next steps for production
4. Write tests covering all functionality
5. Update this README with your example

## Resources

- [Main Documentation](../README.md)
- [Development Guide](../DEVELOPING.md)
- [PydanticAdapter API](../key-value/key-value-aio/src/key_value/aio/adapters/pydantic/)
- [Available Wrappers](../key-value/key-value-aio/src/key_value/aio/wrappers/)
- [Available Stores](../key-value/key-value-aio/src/key_value/aio/stores/)

## Support

For issues, questions, or contributions:

- [GitHub Issues](https://github.com/strawgate/py-key-value/issues)
- [GitHub Discussions](https://github.com/strawgate/py-key-value/discussions)

## License

All examples are licensed under Apache-2.0, same as the main project.
