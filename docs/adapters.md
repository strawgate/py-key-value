# Adapters

Adapters provide specialized interfaces for working with key-value stores. Unlike
wrappers, adapters don't implement the `AsyncKeyValue` protocol - instead, they
provide alternative APIs tailored for specific use cases.

## Available Adapters

| Adapter | Description | Safety Level |
| ------- | ----------- | ------------ |
| [DataclassAdapter](#dataclassadapter) | Type-safe storage/retrieval of dataclass models | ✅ Safe (dataclass-only) |
| [BaseModelAdapter](#basemodeladapter) | Type-safe storage/retrieval of Pydantic BaseModel instances | ✅ Safe (BaseModel-only) |
| [PydanticAdapter](#pydanticadapter) | Storage/retrieval of any pydantic-serializable type | ⚠️ Less safe (any type) |
| [RaiseOnMissingAdapter](#raiseonmissingadapter) | Optional raise-on-missing behavior for get operations | N/A |

## Choosing the Right Adapter

The three primary adapters offer different levels of type safety:

- **DataclassAdapter**: Use when working with Python dataclasses. Provides compile-time and runtime type safety for dataclass types only.

- **BaseModelAdapter**: Use when working with Pydantic BaseModel subclasses. Provides compile-time and runtime type safety for BaseModel types only.

- **PydanticAdapter**: Use when you need maximum flexibility to store any pydantic-serializable type (primitives, datetime, UUID, lists of primitives, etc.). Less type-safe but more flexible.

## Adapters vs Wrappers

**Wrappers:**

- Implement the `AsyncKeyValue` protocol
- Can be stacked and used anywhere a store is expected
- Add transparent functionality (compression, encryption, etc.)
- Don't change the API

**Adapters:**

- Provide a different API
- Cannot be used in place of a store
- Add type safety and specialized behavior
- Transform how you interact with the store

## Adapter Details

### DataclassAdapter

The `DataclassAdapter` provides type-safe storage and retrieval of Python
dataclass models. It automatically handles serialization and validation using
Pydantic for validation.

#### Use Cases

- Type-safe data storage with dataclasses
- Automatic validation on retrieval
- Working with Python's native dataclass decorator
- Ensuring data integrity

#### Basic Example

```python
from dataclasses import dataclass
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.dataclass import DataclassAdapter

@dataclass
class User:
    name: str
    email: str
    age: int

# Create adapter
adapter = DataclassAdapter(
    key_value=MemoryStore(),
    dataclass_type=User
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

---

### BaseModelAdapter

The `BaseModelAdapter` provides type-safe storage and retrieval of Pydantic BaseModel instances. It's the recommended adapter for Pydantic models because it enforces type safety at both compile-time and runtime.

::: key_value.aio.adapters.base_model.BaseModelAdapter
    options:
      show_source: false
      members: true

#### Use Cases

- Type-safe storage of Pydantic BaseModel instances
- When you want compile-time type checking for model types
- Projects using Pydantic for data validation
- Ensuring only BaseModel subclasses are stored

#### Basic Example

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.base_model import BaseModelAdapter

class User(BaseModel):
    name: str
    email: str
    age: int

# Create adapter
adapter = BaseModelAdapter(
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

The `BaseModelAdapter` supports storing lists of BaseModel instances:

```python
from pydantic import BaseModel
from key_value.aio.stores.memory import MemoryStore
from key_value.aio.adapters.base_model import BaseModelAdapter

class User(BaseModel):
    name: str
    email: str

# Create adapter for list of users
adapter = BaseModelAdapter(
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

#### Type Safety Benefits

The `BaseModelAdapter` enforces that only BaseModel subclasses can be used:

```python
from pydantic import BaseModel
from key_value.aio.adapters.base_model import BaseModelAdapter

class User(BaseModel):
    name: str

# ✅ This works - User is a BaseModel
adapter = BaseModelAdapter(pydantic_model=User, key_value=store)

# ✅ This works - list[User] where User is a BaseModel
adapter = BaseModelAdapter(pydantic_model=list[User], key_value=store)

# ❌ This fails at runtime - int is not a BaseModel
adapter = BaseModelAdapter(pydantic_model=int, key_value=store)  # TypeError!

# ❌ This fails at runtime - list[int] inner type is not a BaseModel
adapter = BaseModelAdapter(pydantic_model=list[int], key_value=store)  # TypeError!
```

---

### PydanticAdapter

The `PydanticAdapter` provides storage and retrieval of **any pydantic-serializable type**. Unlike `BaseModelAdapter`, it accepts primitives, collections, datetime objects, and more—not just BaseModel subclasses.

::: key_value.aio.adapters.pydantic.PydanticAdapter
    options:
      show_source: false
      members: true

#### Use Cases

- Storing primitive types (int, str, float, bool)
- Storing datetime, UUID, Decimal, and other common types
- Storing lists of primitives (e.g., `list[int]`, `list[str]`)
- Maximum flexibility when type constraints aren't needed
- Working with any pydantic-serializable type

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

#### Storing Non-Model Types

The `PydanticAdapter` can store types that `BaseModelAdapter` cannot:

```python
from datetime import datetime
from uuid import UUID
from key_value.aio.adapters.pydantic import PydanticAdapter

# ✅ Store primitives
int_adapter = PydanticAdapter(key_value=store, pydantic_model=int)
await int_adapter.put(key="count", value=42, collection="stats")

# ✅ Store datetime
datetime_adapter = PydanticAdapter(key_value=store, pydantic_model=datetime)
await datetime_adapter.put(key="timestamp", value=datetime.now(), collection="events")

# ✅ Store UUID
uuid_adapter = PydanticAdapter(key_value=store, pydantic_model=UUID)
await uuid_adapter.put(key="id", value=UUID("12345678-1234-5678-1234-567812345678"))

# ✅ Store list of primitives
list_adapter = PydanticAdapter(key_value=store, pydantic_model=list[int])
await list_adapter.put(key="scores", value=[10, 20, 30], collection="game")
```

**Note**: `BaseModelAdapter` would reject all of these types because they're not BaseModel subclasses.

---

## BaseModelAdapter vs PydanticAdapter: When to Use Each

### Use BaseModelAdapter When:

✅ You're working exclusively with Pydantic BaseModel subclasses
✅ You want compile-time type safety
✅ You want runtime validation that only BaseModel types are stored
✅ You prefer strict type constraints

**Example Use Case:** A user management system where all entities are Pydantic models:

```python
from pydantic import BaseModel, EmailStr
from key_value.aio.adapters.base_model import BaseModelAdapter

class User(BaseModel):
    id: int
    name: str
    email: EmailStr

class Organization(BaseModel):
    id: int
    name: str
    users: list[User]

# Type-safe: only User instances allowed
user_adapter = BaseModelAdapter[User](
    key_value=store,
    pydantic_model=User
)

# Type-safe: only Organization instances allowed
org_adapter = BaseModelAdapter[Organization](
    key_value=store,
    pydantic_model=Organization
)

# ✅ This works - User is a BaseModel
await user_adapter.put(key="user:1", value=User(id=1, name="Alice", email="alice@example.com"))

# ❌ This fails at type-check time - wrong type
# await user_adapter.put(key="user:1", value="not a user")
```

### Use PydanticAdapter When:

✅ You need to store primitive types (int, str, datetime, UUID, etc.)
✅ You need to store lists of primitives (`list[int]`, `list[str]`)
✅ You're storing a mix of different types
✅ You need maximum flexibility
✅ Type constraints would be too restrictive

**Example Use Case:** A caching system that stores various data types:

```python
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel
from key_value.aio.adapters.pydantic import PydanticAdapter

# Different adapters for different types
session_adapter = PydanticAdapter[UUID](key_value=store, pydantic_model=UUID)
timestamp_adapter = PydanticAdapter[datetime](key_value=store, pydantic_model=datetime)
counter_adapter = PydanticAdapter[int](key_value=store, pydantic_model=int)
tags_adapter = PydanticAdapter[list[str]](key_value=store, pydantic_model=list[str])

# ✅ All of these work with PydanticAdapter
await session_adapter.put(key="session:abc", value=UUID("..."))
await timestamp_adapter.put(key="last-login:user:1", value=datetime.now())
await counter_adapter.put(key="views:page:1", value=1000)
await tags_adapter.put(key="tags:post:1", value=["python", "tutorial"])

# ❌ BaseModelAdapter would reject all of these (not BaseModel types)
```

### Side-by-Side Comparison

```python
from pydantic import BaseModel
from key_value.aio.adapters.base_model import BaseModelAdapter
from key_value.aio.adapters.pydantic import PydanticAdapter

class Product(BaseModel):
    name: str
    price: float

# BaseModelAdapter: Strict type safety
base_adapter = BaseModelAdapter[Product](
    key_value=store,
    pydantic_model=Product
)

# PydanticAdapter: Flexible
pydantic_adapter = PydanticAdapter[Product](
    key_value=store,
    pydantic_model=Product
)

# Both can store Product instances
product = Product(name="Widget", price=29.99)
await base_adapter.put(key="product:1", value=product)
await pydantic_adapter.put(key="product:1", value=product)

# But only PydanticAdapter can be used with non-BaseModel types:

# ✅ PydanticAdapter can do this
price_adapter = PydanticAdapter[float](key_value=store, pydantic_model=float)
await price_adapter.put(key="price:product:1", value=29.99)

# ❌ BaseModelAdapter cannot
# base_adapter = BaseModelAdapter[float](...) # TypeError at runtime!
```

### Key Differences

| Feature | BaseModelAdapter | PydanticAdapter |
|---------|------------------|-----------------|
| **Accepted Types** | Only `BaseModel` or `list[BaseModel]` | Any pydantic-serializable type |
| **Type Safety** | Compile-time + runtime validation | Runtime serialization only |
| **Primitives** | ❌ No (int, str, float, etc.) | ✅ Yes |
| **DateTime/UUID** | ❌ No | ✅ Yes |
| **BaseModel** | ✅ Yes | ✅ Yes |
| **List of Primitives** | ❌ No | ✅ Yes |
| **Type Constraints** | Strict | Flexible |
| **Use Case** | BaseModel-only projects | Mixed-type storage |

---

### RaiseOnMissingAdapter

The `RaiseOnMissingAdapter` changes the behavior of `get` operations to raise an
error instead of returning `None` when a key is not found.

::: key_value.aio.adapters.raise_on_missing.RaiseOnMissingAdapter
    options:
      show_source: false
      members: true

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
