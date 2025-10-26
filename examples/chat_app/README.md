# Chat App Example

A simple async chat application demonstrating py-key-value with the
PydanticAdapter and various wrappers.

## Overview

This example shows how to build a type-safe chat message storage system using
py-key-value. Messages are automatically expired after 24 hours, and operation
statistics are tracked for monitoring.

## Features

- **Type-safe message storage** using PydanticAdapter
- **Automatic message expiration** with TTLClampWrapper (24 hours max)
- **Operation statistics** tracking with StatisticsWrapper
- **Debug logging** with LoggingWrapper
- **Conversation isolation** using collection-based storage

## Architecture

The wrapper stack (applied inside-out):

1. **StatisticsWrapper** - Tracks operation metrics (puts, gets, deletes, hits,
   misses)
2. **TTLClampWrapper** - Enforces maximum TTL of 24 hours for automatic message
   expiration
3. **LoggingWrapper** - Logs all operations for debugging and auditing

## Requirements

- Python 3.10 or newer
- py-key-value-aio
- pydantic

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
from chat_app import ChatApp

async def main():
    chat = ChatApp()

    # Send messages
    msg1_id = await chat.send_message("conv-123", "Alice", "Hello!")
    msg2_id = await chat.send_message("conv-123", "Bob", "Hi Alice!")

    # Retrieve messages
    message1 = await chat.get_message("conv-123", msg1_id)
    print(f"{message1.sender}: {message1.content}")

    # Delete a message
    await chat.delete_message("conv-123", msg1_id)

    # View statistics
    stats = chat.get_statistics()
    print(f"Total messages sent: {stats['total_puts']}")

asyncio.run(main())
```

### Running the Demo

```bash
python chat_app.py
```

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest test_chat_app.py -v
```

## Key Concepts

### PydanticAdapter

The PydanticAdapter provides type-safe storage and retrieval of Pydantic
models:

```python
from pydantic import BaseModel
from key_value.aio.adapters.pydantic import PydanticAdapter

class ChatMessage(BaseModel):
    sender: str
    content: str
    timestamp: datetime

adapter = PydanticAdapter[ChatMessage](
    key_value=store,
    pydantic_model=ChatMessage,
)
```

### Wrapper Composition

Wrappers are applied inside-out, creating a processing pipeline:

```python
# The outermost wrapper (LoggingWrapper) receives requests first
wrapped_store = LoggingWrapper(
    key_value=TTLClampWrapper(
        key_value=StatisticsWrapper(
            key_value=base_store  # Innermost: actual storage
        )
    )
)
```

Request flow: LoggingWrapper → TTLClampWrapper → StatisticsWrapper →
MemoryStore

### Collection-based Storage

Collections provide namespace isolation for different conversations:

```python
# Messages in different conversations don't interfere
await adapter.put(collection="conversation:123", key=msg_id, value=message)
await adapter.put(collection="conversation:456", key=msg_id, value=message)
```

## Next Steps

For production use, consider:

1. **Persistent Storage**: Replace MemoryStore with RedisStore or DiskStore
2. **Encryption**: Add FernetEncryptionWrapper for message privacy
3. **Caching**: Add PassthroughCacheWrapper for multi-tier caching
4. **Retry Logic**: Add RetryWrapper for transient failure handling
5. **Size Limits**: Add LimitSizeWrapper to prevent huge messages

Example with Redis and encryption:

```python
from key_value.aio.stores.redis.store import RedisStore
from key_value.aio.wrappers.encryption.wrapper import FernetEncryptionWrapper

base_store = RedisStore(url="redis://localhost:6379")
wrapped_store = LoggingWrapper(
    key_value=FernetEncryptionWrapper(
        key_value=TTLClampWrapper(
            key_value=StatisticsWrapper(key_value=base_store),
            max_ttl=86400,
        ),
        key=b"your-32-byte-encryption-key-here",
    )
)
```
