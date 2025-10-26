# Adapters

Adapters provide specialized interfaces for working with key-value stores. Unlike
wrappers, adapters don't implement the `AsyncKeyValue` protocol - instead, they
provide alternative APIs tailored for specific use cases.

## Adapters vs Wrappers

**Wrappers:**

- Implement the `AsyncKeyValue` protocol
- Can be stacked and used anywhere a store is expected
- Add transparent functionality (compression, encryption, etc.)
- Don't change the API

**Adapters:**

- Provide a different API interface
- Cannot be used in place of a store
- Add type safety and specialized behavior
- Transform how you interact with the store

## Available Adapters

### PydanticAdapter

The `PydanticAdapter` provides type-safe storage and retrieval of Pydantic
models. It automatically handles serialization and validation, ensuring data
integrity.

::: key_value.aio.adapters.pydantic.PydanticAdapter
    options:
      show_source: false
      members:
        - __init__
        - get
        - get_many
        - put
        - put_many
        - delete
        - delete_many
        - ttl
        - ttl_many

#### Use Cases

- Type-safe data storage
- Automatic validation on retrieval
- Working with complex data models
- Ensuring data integrity

#### Basic Example

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.pydantic import PydanticAdapter

class User(BaseModel):
    name: str
    email: str
    age: int

# Create adapter
adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=User
)

# Store a user (type-safe)
user = User(name="Alice", email="alice@example.com", age=30)
await adapter.put(key="user:123", value=user, collection="users")

# Retrieve and get a validated model
retrieved_user = await adapter.get(key="user:123", collection="users")
if retrieved_user:
    print(retrieved_user.name)  # Type-safe: "Alice"
    print(retrieved_user.email)  # Type-safe: "alice@example.com"
```

#### Storing Lists of Models

The `PydanticAdapter` supports storing lists of Pydantic models:

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.pydantic import PydanticAdapter

class User(BaseModel):
    name: str
    email: str

# Create adapter for list of users
adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=list[User]
)

# Store a list of users
users = [
    User(name="Alice", email="alice@example.com"),
    User(name="Bob", email="bob@example.com"),
]
await adapter.put(key="all-users", value=users, collection="users")

# Retrieve the list
retrieved_users = await adapter.get(key="all-users", collection="users")
if retrieved_users:
    for user in retrieved_users:
        print(user.name)  # Type-safe access
```

#### Validation Error Handling

By default, the adapter returns `None` if validation fails. You can configure it
to raise an error instead:

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.shared.errors import DeserializationError

adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=User,
    raise_on_validation_error=True
)

# Manually corrupt data in the underlying store
await adapter._key_value.put(
    key="user:123",
    value={"name": "Alice"},  # Missing required 'email' field
    collection="users"
)

try:
    user = await adapter.get(key="user:123", collection="users")
except DeserializationError as e:
    print(f"Validation failed: {e}")
```

#### Default Collection

Set a default collection to avoid repeating it in every call:

```python
adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=User,
    default_collection="users"
)

# No need to specify collection
await adapter.put(key="user:123", value=user)
user = await adapter.get(key="user:123")
```

#### Batch Operations

The `PydanticAdapter` supports batch operations for better performance:

```python
# Store multiple users
users = [
    User(name="Alice", email="alice@example.com", age=30),
    User(name="Bob", email="bob@example.com", age=25),
    User(name="Charlie", email="charlie@example.com", age=35),
]

await adapter.put_many(
    keys=["user:1", "user:2", "user:3"],
    values=users,
    collection="users"
)

# Retrieve multiple users
retrieved = await adapter.get_many(
    keys=["user:1", "user:2", "user:3"],
    collection="users"
)

for user in retrieved:
    if user:
        print(user.name)
```

#### TTL Support

The `PydanticAdapter` supports TTL for automatic expiration:

```python
# Store with TTL
await adapter.put(
    key="session:abc",
    value=session_data,
    collection="sessions",
    ttl=3600  # Expires in 1 hour
)

# Get with TTL information
session, ttl = await adapter.ttl(key="session:abc", collection="sessions")
if session:
    print(f"Session expires in {ttl} seconds")
```

#### Complex Models

The `PydanticAdapter` works with complex nested models:

```python
from pydantic import BaseModel
from datetime import datetime

class Address(BaseModel):
    street: str
    city: str
    country: str

class User(BaseModel):
    name: str
    email: str
    address: Address
    created_at: datetime

adapter = PydanticAdapter(
    key_value=MemoryStore(),
    pydantic_model=User
)

user = User(
    name="Alice",
    email="alice@example.com",
    address=Address(
        street="123 Main St",
        city="New York",
        country="USA"
    ),
    created_at=datetime.now()
)

await adapter.put(key="user:123", value=user, collection="users")
retrieved = await adapter.get(key="user:123", collection="users")

if retrieved:
    print(retrieved.address.city)  # Type-safe: "New York"
```

---

### RaiseOnMissingAdapter

The `RaiseOnMissingAdapter` changes the behavior of `get` operations to raise an
error instead of returning `None` when a key is not found.

::: key_value.aio.adapters.raise_on_missing.RaiseOnMissingAdapter
    options:
      show_source: false
      members:
        - __init__
        - get
        - get_many

#### Use Cases

- Enforcing required data
- Fail-fast behavior
- APIs where missing data is an error

#### Example

```python
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.raise_on_missing import RaiseOnMissingAdapter
from key_value.shared.errors import KeyNotFoundError

adapter = RaiseOnMissingAdapter(
    key_value=MemoryStore()
)

# Store a value
await adapter.put(key="user:123", value={"name": "Alice"}, collection="users")

# Get existing key - works normally
user = await adapter.get(key="user:123", collection="users")
print(user)  # {"name": "Alice"}

# Get missing key - raises error
try:
    user = await adapter.get(key="user:999", collection="users")
except KeyNotFoundError as e:
    print(f"Key not found: {e}")
```

#### Batch Operations

The `RaiseOnMissingAdapter` also affects batch operations:

```python
# If any key is missing, raises KeyNotFoundError
try:
    users = await adapter.get_many(
        keys=["user:1", "user:999", "user:3"],
        collection="users"
    )
except KeyNotFoundError as e:
    print(f"One or more keys not found: {e}")
```

---

## Combining Adapters and Wrappers

You can combine adapters with wrappers by wrapping the store before passing it
to the adapter:

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.wrappers.encryption.fernet import FernetEncryptionWrapper
from key_value.aio.wrappers.compression import CompressionWrapper
from key_value.aio.adapters.pydantic import PydanticAdapter
from cryptography.fernet import Fernet

class User(BaseModel):
    name: str
    email: str

# Create encrypted + compressed store
wrapped_store = CompressionWrapper(
    key_value=FernetEncryptionWrapper(
        key_value=MemoryStore(),
        fernet=Fernet(Fernet.generate_key())
    )
)

# Wrap with PydanticAdapter for type safety
adapter = PydanticAdapter(
    key_value=wrapped_store,
    pydantic_model=User
)

# Now you have type-safe, encrypted, and compressed storage!
await adapter.put(key="user:123", value=User(name="Alice", email="alice@example.com"))
```

## Creating Custom Adapters

To create a custom adapter, wrap an `AsyncKeyValue` instance and provide your
own API:

```python
from key_value.aio.protocols.key_value import AsyncKeyValue

class CustomAdapter:
    def __init__(self, key_value: AsyncKeyValue):
        self._key_value = key_value

    async def custom_method(self, key: str) -> dict:
        # Implement custom logic
        value = await self._key_value.get(key=key, collection="custom")
        if value is None:
            return {}
        return value
```

See the [API Reference](api/adapters.md) for complete adapter documentation.
