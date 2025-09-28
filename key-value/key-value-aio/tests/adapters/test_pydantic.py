from datetime import datetime, timezone

import pytest
from pydantic import AnyHttpUrl, BaseModel

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.stores.memory.store import MemoryStore


class User(BaseModel):
    name: str
    age: int
    email: str


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
SAMPLE_PRODUCT: Product = Product(name="Widget", price=29.99, quantity=10, url=AnyHttpUrl(url="https://example.com"))
SAMPLE_ORDER: Order = Order(created_at=datetime.now(), updated_at=datetime.now(), user=SAMPLE_USER, product=SAMPLE_PRODUCT, paid=False)


class TestPydanticAdapter:
    @pytest.fixture
    async def store(self) -> MemoryStore:
        return MemoryStore()

    @pytest.fixture
    async def user_adapter(self, store: MemoryStore) -> PydanticAdapter[User]:
        return PydanticAdapter[User](key_value=store, pydantic_model=User)

    @pytest.fixture
    async def product_adapter(self, store: MemoryStore) -> PydanticAdapter[Product]:
        return PydanticAdapter[Product](key_value=store, pydantic_model=Product)

    @pytest.fixture
    async def order_adapter(self, store: MemoryStore) -> PydanticAdapter[Order]:
        return PydanticAdapter[Order](key_value=store, pydantic_model=Order)

    async def test_simple_adapter(self, user_adapter: PydanticAdapter[User]):
        await user_adapter.put(collection="test", key="test", value=SAMPLE_USER)
        cached_user: User | None = await user_adapter.get(collection="test", key="test")
        assert cached_user == SAMPLE_USER

        assert await user_adapter.delete(collection="test", key="test")

        cached_user = await user_adapter.get(collection="test", key="test")
        assert cached_user is None

    async def test_complex_adapter(self, order_adapter: PydanticAdapter[Order]):
        await order_adapter.put(collection="test", key="test", value=SAMPLE_ORDER, ttl=10)
        cached_order: Order | None = await order_adapter.get(collection="test", key="test")
        assert cached_order == SAMPLE_ORDER

        assert await order_adapter.delete(collection="test", key="test")
        cached_order = await order_adapter.get(collection="test", key="test")
        assert cached_order is None
