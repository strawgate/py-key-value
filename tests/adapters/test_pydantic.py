from collections.abc import Callable
from datetime import datetime, timezone
from logging import LogRecord
from typing import Any

import pytest
from inline_snapshot import snapshot
from pydantic import AnyHttpUrl, BaseModel

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.errors import DeserializationError
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


class TestPydanticAdapter:
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    async def user_adapter(self, store: MemoryStore) -> PydanticAdapter[User]:
        return PydanticAdapter[User](key_value=store, pydantic_model=User)

    @pytest.fixture
    async def updated_user_adapter(self, store: MemoryStore) -> PydanticAdapter[UpdatedUser]:
        return PydanticAdapter[UpdatedUser](key_value=store, pydantic_model=UpdatedUser)

    @pytest.fixture
    async def product_adapter(self, store: MemoryStore) -> PydanticAdapter[Product]:
        return PydanticAdapter[Product](key_value=store, pydantic_model=Product)

    @pytest.fixture
    async def product_list_adapter(self, store: MemoryStore) -> PydanticAdapter[list[Product]]:
        return PydanticAdapter[list[Product]](key_value=store, pydantic_model=list[Product])

    @pytest.fixture
    async def order_adapter(self, store: MemoryStore) -> PydanticAdapter[Order]:
        return PydanticAdapter[Order](key_value=store, pydantic_model=Order)

    async def test_simple_adapter(self, user_adapter: PydanticAdapter[User]):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        cached_user: User | None = await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_user == SAMPLE_USER

        assert await user_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)

        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_simple_adapter_with_default(self, user_adapter: PydanticAdapter[User]):
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER

        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER_2)
        assert await user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY, default=SAMPLE_USER) == SAMPLE_USER_2

        assert await user_adapter.get_many(collection=TEST_COLLECTION, keys=[TEST_KEY, TEST_KEY_2], default=SAMPLE_USER) == snapshot(
            [SAMPLE_USER_2, SAMPLE_USER]
        )

    async def test_simple_adapter_with_list(self, product_list_adapter: PydanticAdapter[list[Product]]):
        await product_list_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=[SAMPLE_PRODUCT, SAMPLE_PRODUCT])
        cached_products: list[Product] | None = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_products == [SAMPLE_PRODUCT, SAMPLE_PRODUCT]

        assert await product_list_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_simple_adapter_with_validation_error_ignore(
        self, user_adapter: PydanticAdapter[User], updated_user_adapter: PydanticAdapter[UpdatedUser]
    ):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)

        updated_user = await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert updated_user is None

    async def test_simple_adapter_with_validation_error_raise(
        self, user_adapter: PydanticAdapter[User], updated_user_adapter: PydanticAdapter[UpdatedUser]
    ):
        await user_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_USER)
        updated_user_adapter._raise_on_validation_error = True
        with pytest.raises(DeserializationError):
            await updated_user_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)

    async def test_complex_adapter(self, order_adapter: PydanticAdapter[Order]):
        await order_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=SAMPLE_ORDER, ttl=10)
        assert await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) == SAMPLE_ORDER

        assert await order_adapter.delete(collection=TEST_COLLECTION, key=TEST_KEY)
        assert await order_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY) is None

    async def test_complex_adapter_with_list(self, product_list_adapter: PydanticAdapter[list[Product]], store: MemoryStore):
        await product_list_adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=[SAMPLE_PRODUCT, SAMPLE_PRODUCT], ttl=10)
        cached_products: list[Product] | None = await product_list_adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert cached_products == [SAMPLE_PRODUCT, SAMPLE_PRODUCT]

        # We need to ensure our memory store doesnt hold an entry with an array
        raw_collection = store._cache.get(TEST_COLLECTION)
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
        self, user_adapter: PydanticAdapter[User], updated_user_adapter: PydanticAdapter[UpdatedUser], caplog: pytest.LogCaptureFixture
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
        assert model_type_from_log_record(record) == "pydantic-serializable value"

        errors = errors_from_log_record(record)
        assert len(errors) == 1
        assert "is_admin" in str(errors[0])

    async def test_list_validation_error_logging(
        self, product_list_adapter: PydanticAdapter[list[Product]], store: MemoryStore, caplog: pytest.LogCaptureFixture
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
        assert model_type_from_log_record(record) == "pydantic-serializable value"
        error = error_from_log_record(record)
        assert "missing 'items' wrapper" in str(error)

    async def test_adapter_with_callable_field(self, store: MemoryStore):
        """Test that PydanticAdapter works with models containing Callable fields.

        This tests the fix for issue #279 where Pydantic's json_schema() would fail
        with 'Cannot generate a JsonSchema for core_schema.CallableSchema' when
        models contain Callable fields. The fix allows adapter creation to succeed
        by skipping non-JSON-serializable fields when generating the schema.

        Note: Callable fields cannot be serialized to JSON, so they must be optional
        (with None default) for storage/retrieval to work.
        """

        class ModelWithCallable(BaseModel):
            name: str
            callback: Callable[[Any], str] | None = None

        # This would previously fail with:
        # PydanticInvalidForJsonSchema: Cannot generate a JsonSchema for core_schema.CallableSchema
        adapter = PydanticAdapter[ModelWithCallable](key_value=store, pydantic_model=ModelWithCallable)

        # Test storage/retrieval with callback=None (the serializable case)
        model = ModelWithCallable(name="test", callback=None)
        await adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=model)

        result = await adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert result is not None
        assert result.name == "test"
        assert result.callback is None

    async def test_adapter_with_multiple_non_json_types(self, store: MemoryStore):
        """Test that PydanticAdapter works with models containing multiple non-JSON-serializable fields.

        This tests that the schema generator correctly handles multiple Callable fields.
        """

        class ModelWithMultipleCallables(BaseModel):
            name: str
            on_success: Callable[[str], None] | None = None
            on_error: Callable[[Exception], None] | None = None
            validator: Callable[[Any], bool] | None = None

        # Creating the adapter should succeed (schema generation works)
        adapter = PydanticAdapter[ModelWithMultipleCallables](key_value=store, pydantic_model=ModelWithMultipleCallables)

        # Test basic storage and retrieval
        model = ModelWithMultipleCallables(name="multi_callback_test")
        await adapter.put(collection=TEST_COLLECTION, key=TEST_KEY, value=model)

        result = await adapter.get(collection=TEST_COLLECTION, key=TEST_KEY)
        assert result is not None
        assert result.name == "multi_callback_test"
        assert result.on_success is None
        assert result.on_error is None
        assert result.validator is None
