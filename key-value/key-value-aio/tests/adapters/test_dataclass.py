from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from inline_snapshot import snapshot
from key_value.shared.errors import DeserializationError

from key_value.aio.adapters.dataclass import DataclassAdapter
from key_value.aio.stores.memory.store import MemoryStore


@dataclass
class User:
    name: str
    age: int
    email: str


@dataclass
class UpdatedUser:
    name: str
    age: int
    email: str
    is_admin: bool


@dataclass
class Product:
    name: str
    price: float
    quantity: int


@dataclass
class Address:
    street: str
    city: str
    zip_code: str


@dataclass
class UserWithAddress:
    name: str
    age: int
    address: Address


@dataclass
class Order:
    created_at: datetime
    updated_at: datetime
    user: User
    product: Product
    paid: bool = False


FIXED_CREATED_AT: datetime = datetime(year=2021, month=1, day=1, hour=12, minute=0, second=0, tzinfo=timezone.utc)
FIXED_UPDATED_AT: datetime = datetime(year=2021, month=1, day=1, hour=15, minute=0, second=0, tzinfo=timezone.utc)

SAMPLE_USER: User = User(name="John Doe", email="john.doe@example.com", age=30)
SAMPLE_USER_2: User = User(name="Jane Doe", email="jane.doe@example.com", age=25)
SAMPLE_PRODUCT: Product = Product(name="Widget", price=29.99, quantity=10)
SAMPLE_ADDRESS: Address = Address(street="123 Main St", city="Springfield", zip_code="12345")
SAMPLE_USER_WITH_ADDRESS: UserWithAddress = UserWithAddress(name="John Doe", age=30, address=SAMPLE_ADDRESS)
SAMPLE_ORDER: Order = Order(created_at=FIXED_CREATED_AT, updated_at=FIXED_UPDATED_AT, user=SAMPLE_USER, product=SAMPLE_PRODUCT, paid=False)

TEST_COLLECTION: str = "test_collection"
TEST_KEY: str = "test_key"
TEST_KEY_2: str = "test_key_2"


class TestDataclassAdapter:
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    async def user_adapter(self, store: MemoryStore) -> DataclassAdapter[User]:
        return DataclassAdapter[User](key_value=store, dataclass_type=User)

    @pytest.fixture
    async def updated_user_adapter(self, store: MemoryStore) -> DataclassAdapter[UpdatedUser]:
        return DataclassAdapter[UpdatedUser](key_value=store, dataclass_type=UpdatedUser)

    @pytest.fixture
    async def product_adapter(self, store: MemoryStore) -> DataclassAdapter[Product]:
        return DataclassAdapter[Product](key_value=store, dataclass_type=Product)

    @pytest.fixture
    async def product_list_adapter(self, store: MemoryStore) -> DataclassAdapter[list[Product]]:
        return DataclassAdapter[list[Product]](key_value=store, dataclass_type=list[Product])

    @pytest.fixture
    async def user_with_address_adapter(self, store: MemoryStore) -> DataclassAdapter[UserWithAddress]:
        return DataclassAdapter[UserWithAddress](key_value=store, dataclass_type=UserWithAddress)

    @pytest.fixture
    async def order_adapter(self, store: MemoryStore) -> DataclassAdapter[Order]:
        return DataclassAdapter[Order](key_value=store, dataclass_type=Order)

    async def test_simple_adapter(self, user_adapter: DataclassAdapter[User]):
        """Test basic put/get/delete operations with a simple dataclass."""
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        cached_user: User | None = await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_user == SAMPLE_USER

        assert await user_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)

        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_simple_adapter_with_default(self, user_adapter: DataclassAdapter[User]):
        """Test default value handling."""
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER

        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER_2)
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER_2

        assert await user_adapter.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY, TEST_KEY_2], default=SAMPLE_USER) == snapshot(
            [SAMPLE_USER_2, SAMPLE_USER]
        )

    async def test_simple_adapter_with_validation_error_ignore(
        self, user_adapter: DataclassAdapter[User], updated_user_adapter: DataclassAdapter[UpdatedUser]
    ):
        """Test that validation errors return None when raise_on_validation_error is False."""
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)

        # UpdatedUser requires is_admin field which doesn't exist in stored User
        updated_user = await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert updated_user is None

    async def test_simple_adapter_with_validation_error_raise(
        self, user_adapter: DataclassAdapter[User], updated_user_adapter: DataclassAdapter[UpdatedUser]
    ):
        """Test that validation errors raise DeserializationError when raise_on_validation_error is True."""
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        updated_user_adapter._raise_on_validation_error = True  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(DeserializationError):
            await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)

    async def test_nested_dataclass(self, user_with_address_adapter: DataclassAdapter[UserWithAddress]):
        """Test that nested dataclasses are properly serialized and deserialized."""
        await user_with_address_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER_WITH_ADDRESS)
        cached_user: UserWithAddress | None = await user_with_address_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_user == SAMPLE_USER_WITH_ADDRESS
        assert cached_user is not None
        assert cached_user.address.street == "123 Main St"

    async def test_complex_adapter(self, order_adapter: DataclassAdapter[Order]):
        """Test complex dataclass with nested objects and TTL."""
        await order_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_ORDER, ttl=10)
        cached_order: Order | None = await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_order == SAMPLE_ORDER

        assert await order_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_complex_adapter_with_list(self, product_list_adapter: DataclassAdapter[list[Product]], store: MemoryStore):
        """Test list dataclass serialization with proper wrapping."""
        await product_list_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=[SAMPLE_PRODUCT, SAMPLE_PRODUCT], ttl=10)
        cached_products: list[Product] | None = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_products == [SAMPLE_PRODUCT, SAMPLE_PRODUCT]

        # We need to ensure our memory store doesn't hold an entry with an array
        raw_collection = store._cache.get(TEST_COLLECTION)  # pyright: ignore[reportPrivateUsage]
        assert raw_collection is not None

        raw_entry = raw_collection.get(TEST_KEY)
        assert raw_entry is not None
        assert isinstance(raw_entry.value, dict)
        assert raw_entry.value == snapshot(
            {"items": [{"name": "Widget", "price": 29.99, "quantity": 10}, {"name": "Widget", "price": 29.99, "quantity": 10}]}
        )

        assert await product_list_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_batch_operations(self, user_adapter: DataclassAdapter[User]):
        """Test batch put/get/delete operations."""
        keys = [TEST_KEY, TEST_KEY_2]
        users = [SAMPLE_USER, SAMPLE_USER_2]

        # Test put_many
        await user_adapter.put_many(collection=TEST_COLLECTION, keys=keys, values=users)

        # Test get_many
        cached_users = await user_adapter.get_many(collection=TEST_COLLECTION, keys=keys)
        assert cached_users == users

        # Test delete_many
        deleted_count = await user_adapter.delete_many(collection=TEST_COLLECTION, keys=keys)
        assert deleted_count == 2

        # Verify deletion
        cached_users_after_delete = await user_adapter.get_many(collection=TEST_COLLECTION, keys=keys)
        assert cached_users_after_delete == [None, None]

    async def test_ttl_operations(self, user_adapter: DataclassAdapter[User]):
        """Test TTL-related operations."""
        # Test single TTL
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER, ttl=10)
        user, ttl = await user_adapter.ttl(collection=TEST_COLLECTION, key=TEST_KEY)
        assert user == SAMPLE_USER
        assert ttl is not None
        assert ttl > 0

        # Test ttl_many
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY_2, value=SAMPLE_USER_2, ttl=20)
        ttl_results = await user_adapter.ttl_many(collection=TEST_COLLECTION, keys=[TEST_KEY, TEST_KEY_2])
        assert len(ttl_results) == 2
        assert ttl_results[0][0] == SAMPLE_USER
        assert ttl_results[1][0] == SAMPLE_USER_2

    async def test_dataclass_validation_on_init(self, store: MemoryStore):
        """Test that non-dataclass types are rejected."""
        with pytest.raises(TypeError, match="is not a dataclass"):
            DataclassAdapter[str](key_value=store, dataclass_type=str)  # type: ignore[type-var]

    async def test_default_collection(self, store: MemoryStore):
        """Test that default collection is used when not specified."""
        adapter = DataclassAdapter[User](key_value=store, dataclass_type=User, default_collection=TEST_COLLECTION)

        await adapter.put(key=TEST_KEY, value=SAMPLE_USER)
        cached_user = await adapter.get(key=TEST_KEY)
        assert cached_user == SAMPLE_USER

        assert await adapter.delete(key=TEST_KEY)
