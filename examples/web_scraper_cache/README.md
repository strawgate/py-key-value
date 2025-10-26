# Web Scraper Cache Example

A web scraper cache demonstrating security and resilience patterns with
py-key-value, including encryption, size limits, and fallback handling.

## Overview

This example shows how to build a secure and robust cache for web scraping
results. Scraped pages are encrypted before storage, with size limits to
prevent memory issues and automatic fallback to memory storage if disk
operations fail.

## Features

- **Type-safe scraped data storage** using PydanticAdapter
- **Encrypted storage** with FernetEncryptionWrapper for data privacy
- **Size limits** with LimitSizeWrapper (5MB per page)
- **TTL clamping** with TTLClampWrapper (1 hour - 7 days)
- **Fallback resilience** with FallbackWrapper (memory fallback if disk fails)
- **URL-based caching** with SHA-256 hash keys

## Architecture

The wrapper stack (applied inside-out):

1. **TTLClampWrapper** - Enforces cache duration between 1 hour and 7 days
2. **LimitSizeWrapper** - Rejects pages larger than 5MB to prevent memory/disk
   issues
3. **FernetEncryptionWrapper** - Encrypts all cached content for privacy
4. **FallbackWrapper** - Falls back to memory storage if disk operations fail

Data flow:
- **Write**: Data → Size check → Encrypt → Store (disk or memory fallback)
- **Read**: Retrieve → Decrypt → Return data

## Requirements

- Python 3.10 or newer
- py-key-value-aio
- pydantic
- cryptography

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or using uv
uv pip install -e .
```

## Usage

### Basic Example

```python
import asyncio
from cryptography.fernet import Fernet
from scraper import WebScraperCache

async def main():
    # Generate encryption key (store securely in production!)
    key = Fernet.generate_key()

    cache = WebScraperCache(
        cache_dir=".scraper_cache",
        encryption_key=key
    )

    # Cache a page
    url = "https://example.com/page"
    content = "<html><body>Page content</body></html>"
    headers = {"content-type": "text/html"}

    cached = await cache.cache_page(url, content, headers, ttl=3600)

    if cached:
        # Retrieve from cache
        page = await cache.get_cached_page(url)
        print(f"Content: {page.content}")
        print(f"Scraped at: {page.scraped_at}")

    await cache.cleanup()

asyncio.run(main())
```

### Running the Demo

```bash
python scraper.py
```

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest test_scraper.py -v
```

## Key Concepts

### Encryption

FernetEncryptionWrapper provides symmetric encryption:

```python
from cryptography.fernet import Fernet

# Generate a key (do this once, store securely)
key = Fernet.generate_key()

# Use the key for encryption
encrypted_store = FernetEncryptionWrapper(
    key_value=base_store,
    key=key
)
```

**Important**: Store the encryption key securely! Data encrypted with one key
cannot be decrypted with a different key.

### Size Limits

LimitSizeWrapper prevents oversized entries:

```python
limited_store = LimitSizeWrapper(
    key_value=base_store,
    max_size_bytes=5 * 1024 * 1024  # 5MB limit
)
```

Attempts to store larger values will raise an exception or return False.

### Fallback Pattern

FallbackWrapper provides resilience:

```python
fallback_store = FallbackWrapper(
    key_value=primary_store,    # Try this first
    fallback=secondary_store    # Use this if primary fails
)
```

Use cases:
- Disk failures → Memory fallback
- Remote failures → Local fallback
- Complex store → Simple store fallback

### TTL Clamping

TTLClampWrapper enforces min/max TTL:

```python
clamped_store = TTLClampWrapper(
    key_value=base_store,
    min_ttl=3600,           # 1 hour minimum
    max_ttl=7 * 24 * 3600   # 7 days maximum
)
```

Benefits:
- Prevents too-short TTL (cache thrashing)
- Prevents too-long TTL (stale data)
- Enforces consistent caching policy

### URL Hashing

URLs are hashed for safe cache keys:

```python
import hashlib

def url_to_key(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()
```

Advantages:
- Filesystem-safe keys
- Consistent key generation
- Privacy (URLs not stored in plaintext as keys)

## Security Considerations

### Encryption Key Management

**Never hardcode encryption keys!** Options for key storage:

```python
# Option 1: Environment variable
import os
key = os.environ.get("SCRAPER_CACHE_KEY").encode()

# Option 2: Secrets manager (AWS Secrets Manager, etc.)
from key_value.aio.stores.vault.store import VaultStore
# Use Vault or similar for key storage

# Option 3: Key file with restricted permissions
from pathlib import Path
key = Path("/secure/location/cache.key").read_bytes()
```

### Content Validation

Always validate scraped content before caching:

```python
def is_safe_content(content: str) -> bool:
    # Check for malicious content
    # Validate structure
    # Check size
    return True

if is_safe_content(content):
    await cache.cache_page(url, content, headers)
```

### Access Control

Restrict cache directory permissions:

```bash
# Linux/macOS
chmod 700 .scraper_cache

# Or in Python
import os
os.chmod(".scraper_cache", 0o700)
```

## Performance Considerations

### Cache Warming

Pre-populate cache with frequently accessed pages:

```python
async def warm_cache(cache: WebScraperCache, urls: list[str]):
    for url in urls:
        if not await cache.is_cached(url):
            content, headers = await scrape_page(url)
            await cache.cache_page(url, content, headers)
```

### Batch Operations

Process multiple pages efficiently:

```python
async def cache_multiple(cache: WebScraperCache, pages: list[tuple]):
    tasks = [
        cache.cache_page(url, content, headers)
        for url, content, headers in pages
    ]
    return await asyncio.gather(*tasks)
```

### Memory Usage

Monitor memory cache size (fallback store):

```python
# In production, implement memory limits
# or periodic cleanup for the fallback memory store
```

## Next Steps

For production use, consider:

1. **Database Backend**: Use PostgreSQL, MongoDB, or Elasticsearch for
   searchability
2. **Content Extraction**: Cache extracted data, not raw HTML
3. **Deduplication**: Detect and handle duplicate content
4. **Rate Limiting**: Add delays between scraping operations
5. **Monitoring**: Track cache hit rates, storage usage, and errors

Example with Redis and content extraction:

```python
from key_value.aio.stores.redis.store import RedisStore

redis_store = RedisStore(url="redis://localhost:6379")

cache = WebScraperCache(
    cache_dir="historical_data",
    encryption_key=key
)

# Extract and cache only relevant data
from bs4 import BeautifulSoup

def extract_content(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

content = extract_content(raw_html)
await cache.cache_page(url, content, headers)
```

## Integration with Scraping Libraries

### With aiohttp

```python
import aiohttp

async def scrape_and_cache(cache: WebScraperCache, url: str):
    # Check cache first
    if await cache.is_cached(url):
        return await cache.get_cached_page(url)

    # Scrape if not cached
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            headers = dict(response.headers)

            await cache.cache_page(url, content, headers)

            return await cache.get_cached_page(url)
```

### With httpx

```python
import httpx

async def scrape_and_cache(cache: WebScraperCache, url: str):
    if await cache.is_cached(url):
        return await cache.get_cached_page(url)

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        await cache.cache_page(url, response.text, dict(response.headers))

        return await cache.get_cached_page(url)
```
