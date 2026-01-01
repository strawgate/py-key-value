from datetime import datetime, timezone
from logging import LogRecord

import pytest
from inline_snapshot import snapshot
from key_value.shared.errors import DeserializationError
from pydantic import AnyHttpUrl, BaseModel

from key_value.aio.adapters.base_model import BaseModelAdapter
from key_value.aio.stores.memory.store import MemoryStore


class User(BaseModel):
    name: str
    age: int
    email: str


class UpdatedUser(User):
    is_admin: bool


class Product(BaseModel):
    name: str
    price: float
    quantity: int
    url: AnyHttpUrl


class Order(BaseModel):
    created_at: datetime
    updated_at: datetime
    user: User
    product: Product
    paid: bool


FIXED_CREATED_AT: datetime = datetime(year=2021, month=1, day=1, hour=12, minute=0, second=0, tzinfo=timezone.utc)
FIXED_UPDATED_AT: datetime = datetime(year=2021, month=1, day=1, hour=15, minute=0, second=0, tzinfo=timezone.utc)

SAMPLE_USER: User = User(name="John Doe", email="john.doe@example.com", age=30)
SAMPLE_USER_2: User = User(name="Jane Doe", email="jane.doe@example.com", age=25)
SAMPLE_PRODUCT: Product = Product(name="Widget", price=29.99, quantity=10, url=AnyHttpUrl(url="https://example.com"))
SAMPLE_ORDER: Order = Order(created_at=datetime.now(), updated_at=datetime.now(), user=SAMPLE_USER, product=SAMPLE_PRODUCT, paid=False)

TEST_COLLECTION: str = "test_collection"
TEST_KEY: str = "test_key"
TEST_KEY_2: str = "test_key_2"


def model_type_from_log_record(record: LogRecord) -> str:
    if not hasattr(record, "model_type"):
        msg = "Log record does not have a model_type attribute"
        raise ValueError(msg)
    return record.model_type  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]


def error_from_log_record(record: LogRecord) -> str:
    if not hasattr(record, "error"):
        msg = "Log record does not have an error attribute"
        raise ValueError(msg)
    return record.error  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]


def errors_from_log_record(record: LogRecord) -> list[str]:
    if not hasattr(record, "errors"):
        msg = "Log record does not have an errors attribute"
        raise ValueError(msg)
    return record.errors  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportAttributeAccessIssue]


class TestBaseModelAdapter:
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    async def user_adapter(self, store: MemoryStore) -> BaseModelAdapter[User]:
        return BaseModelAdapter[User](key_value=store, pydantic_model=User)

    @pytest.fixture
    async def updated_user_adapter(self, store: MemoryStore) -> BaseModelAdapter[UpdatedUser]:
        return BaseModelAdapter[UpdatedUser](key_value=store, pydantic_model=UpdatedUser)

    @pytest.fixture
    async def product_adapter(self, store: MemoryStore) -> BaseModelAdapter[Product]:
        return BaseModelAdapter[Product](key_value=store, pydantic_model=Product)

    @pytest.fixture
    async def product_list_adapter(self, store: MemoryStore) -> BaseModelAdapter[list[Product]]:
        return BaseModelAdapter[list[Product]](key_value=store, pydantic_model=list[Product])

    @pytest.fixture
    async def order_adapter(self, store: MemoryStore) -> BaseModelAdapter[Order]:
        return BaseModelAdapter[Order](key_value=store, pydantic_model=Order)

    async def test_simple_adapter(self, user_adapter: BaseModelAdapter[User]):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        cached_user: User | None = await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_user == SAMPLE_USER

        assert await user_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)

        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_simple_adapter_with_default(self, user_adapter: BaseModelAdapter[User]):
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER

        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER_2)
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER_2

        assert await user_adapter.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY, TEST_KEY_2], default=SAMPLE_USER) == snapshot(
            [SAMPLE_USER_2, SAMPLE_USER]
        )

    async def test_simple_adapter_with_list(self, product_list_adapter: BaseModelAdapter[list[Product]]):
        await product_list_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=[SAMPLE_PRODUCT, SAMPLE_PRODUCT])
        cached_products: list[Product] | None = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_products == [SAMPLE_PRODUCT, SAMPLE_PRODUCT]

        assert await product_list_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_simple_adapter_with_validation_error_ignore(
        self, user_adapter: BaseModelAdapter[User], updated_user_adapter: BaseModelAdapter[UpdatedUser]
    ):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)

        updated_user = await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert updated_user is None

    async def test_simple_adapter_with_validation_error_raise(
        self, user_adapter: BaseModelAdapter[User], updated_user_adapter: BaseModelAdapter[UpdatedUser]
    ):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        updated_user_adapter._raise_on_validation_error = True  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(DeserializationError):
            await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)

    async def test_complex_adapter(self, order_adapter: BaseModelAdapter[Order]):
        await order_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_ORDER, ttl=10)
        assert await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) == SAMPLE_ORDER

        assert await order_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_complex_adapter_with_list(self, product_list_adapter: BaseModelAdapter[list[Product]], store: MemoryStore):
        await product_list_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=[SAMPLE_PRODUCT, SAMPLE_PRODUCT], ttl=10)
        cached_products: list[Product] | None = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_products == [SAMPLE_PRODUCT, SAMPLE_PRODUCT]

        # We need to ensure our memory store doesnt hold an entry with an array
        raw_collection = store._cache.get(TEST_COLLECTION)  # pyright: ignore[reportPrivateUsage]
        assert raw_collection is not None

        raw_entry = raw_collection.get(TEST_KEY)
        assert raw_entry is not None
        assert isinstance(raw_entry.value, dict)
        assert raw_entry.value == snapshot(
            {
                "items": [
                    {"name": "Widget", "price": 29.99, "quantity": 10, "url": "https://example.com/"},
                    {"name": "Widget", "price": 29.99, "quantity": 10, "url": "https://example.com/"},
                ]
            }
        )

        assert await product_list_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_validation_error_logging(
        self, user_adapter: BaseModelAdapter[User], updated_user_adapter: BaseModelAdapter[UpdatedUser], caplog: pytest.LogCaptureFixture
    ):
        """Test that validation errors are logged when raise_on_validation_error=False."""
        import logging

        # Store a User, then try to retrieve as UpdatedUser (missing is_admin field)
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)

        with caplog.at_level(logging.ERROR):
            updated_user = await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)

        # Should return None due to validation failure
        assert updated_user is None

        # Check that an error was logged
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "ERROR"
        assert "Validation failed" in record.message
        assert model_type_from_log_record(record) == "BaseModel"

        errors = errors_from_log_record(record)
        assert len(errors) == 1
        assert "is_admin" in str(errors[0])

    async def test_list_validation_error_logging(
        self, product_list_adapter: BaseModelAdapter[list[Product]], store: MemoryStore, caplog: pytest.LogCaptureFixture
    ):
        """Test that missing 'items' wrapper is logged for list models."""
        import logging

        # Manually store invalid data (missing 'items' wrapper)
        await store.put(collection=TEST_COLLECTION, key=TEST_KEY, value={"invalid": "data"})

        with caplog.at_level(logging.ERROR):
            result = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)

        # Should return None due to missing 'items' wrapper
        assert result is None

        # Check that an error was logged
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "ERROR"
        assert "Missing 'items' wrapper" in record.message
        assert model_type_from_log_record(record) == "BaseModel"
        error = error_from_log_record(record)
        assert "missing 'items' wrapper" in str(error)
