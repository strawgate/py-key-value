import contextlib

# Store implementations
with contextlib.suppress(ImportError):
    from kv_store_adapter.stores.memory import MemoryStore

with contextlib.suppress(ImportError):
    from kv_store_adapter.stores.disk import DiskStore

with contextlib.suppress(ImportError):
    from kv_store_adapter.stores.redis import RedisStore

with contextlib.suppress(ImportError):
    from kv_store_adapter.stores.elasticsearch import ElasticsearchStore

with contextlib.suppress(ImportError):
    from kv_store_adapter.stores.duckdb import DuckDBStore

from kv_store_adapter.stores.null import NullStore
from kv_store_adapter.stores.simple import SimpleStore, SimpleJSONStore

# Adapters and wrappers
with contextlib.suppress(ImportError):
    from kv_store_adapter.adapters.pydantic import PydanticAdapter

from kv_store_adapter.stores.wrappers.clamp_ttl import TTLClampWrapper
from kv_store_adapter.stores.wrappers.passthrough_cache import PassthroughCacheWrapper
from kv_store_adapter.stores.wrappers.prefix_collection import PrefixCollectionWrapper
from kv_store_adapter.stores.wrappers.prefix_key import PrefixKeyWrapper
from kv_store_adapter.stores.wrappers.single_collection import SingleCollectionWrapper
from kv_store_adapter.stores.wrappers.statistics import StatisticsWrapper

__all__ = [
    # Optional stores (import fails silently if dependencies missing)
    "DiskStore",
    "DuckDBStore",
    "ElasticsearchStore",
    "MemoryStore",
    "RedisStore",
    # Core stores (always available)
    "NullStore",
    "SimpleJSONStore",
    "SimpleStore",
    # Wrappers (always available)
    "PassthroughCacheWrapper",
    "PrefixCollectionWrapper",
    "PrefixKeyWrapper",
    "SingleCollectionWrapper",
    "StatisticsWrapper",
    "TTLClampWrapper",
    # Adapters (optional)
    "PydanticAdapter",
]