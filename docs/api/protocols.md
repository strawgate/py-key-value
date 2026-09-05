# Protocols

The `AsyncKeyValue` protocol defines the interface that all stores and wrappers
must implement. This protocol-based design allows for maximum flexibility and
composability.

## AsyncKeyValue Protocol

::: key_value.aio.protocols.key_value.AsyncKeyValue
    options:
      show_source: true
      members: true
      show_root_heading: true

## Optional Atomic Conditional Writes

`AsyncPutIfAbsentProtocol` is implemented only by stores that can atomically
check for a missing key and write it. Check the capability at runtime before
calling `put_if_absent()`.

```python
from key_value.aio.protocols import AsyncPutIfAbsentProtocol

if isinstance(store, AsyncPutIfAbsentProtocol):
    stored = await store.put_if_absent(
        key="request-123",
        value={"status": "started"},
        collection="idempotency",
        ttl=300,
    )
```

::: key_value.aio.protocols.key_value.AsyncPutIfAbsentProtocol
    options:
      show_source: true
      members: true
      show_root_heading: true
