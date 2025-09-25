
"""KV Store Adapter - A pluggable interface for Key-Value Stores."""


def __getattr__(name: str):
    """Lazy import for optional store implementations."""
    # Redis Store
    if name == "RedisStore":
        try:
            from kv_store_adapter.stores.redis import RedisStore
            return RedisStore
        except ImportError as e:
            raise ImportError(f"RedisStore requires redis to be installed: {e}") from e
    
    # Valkey Store  
    elif name == "ValkeyStore":
        try:
            from kv_store_adapter.stores.valkey import ValkeyStore
            return ValkeyStore
        except ImportError as e:
            raise ImportError(f"ValkeyStore requires valkey to be installed: {e}") from e
    
    # Memory Store
    elif name == "MemoryStore":
        try:
            from kv_store_adapter.stores.memory import MemoryStore
            return MemoryStore
        except ImportError as e:
            raise ImportError(f"MemoryStore requires cachetools to be installed: {e}") from e
    
    # Disk Store
    elif name == "DiskStore":
        try:
            from kv_store_adapter.stores.disk import DiskStore
            return DiskStore
        except ImportError as e:
            raise ImportError(f"DiskStore requires diskcache to be installed: {e}") from e
    
    # Elasticsearch Store
    elif name == "ElasticsearchStore":
        try:
            from kv_store_adapter.stores.elasticsearch import ElasticsearchStore
            return ElasticsearchStore
        except ImportError as e:
            raise ImportError(f"ElasticsearchStore requires elasticsearch and aiohttp to be installed: {e}") from e
    
    # Simple Stores
    elif name == "SimpleStore":
        from kv_store_adapter.stores.simple import SimpleStore
        return SimpleStore
    elif name == "SimpleJSONStore":
        from kv_store_adapter.stores.simple import SimpleJSONStore
        return SimpleJSONStore
    
    # Null Store
    elif name == "NullStore":
        from kv_store_adapter.stores.null import NullStore
        return NullStore
    
    # If not found, raise AttributeError
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    "RedisStore",
    "ValkeyStore", 
    "MemoryStore",
    "DiskStore",
    "ElasticsearchStore",
    "SimpleStore",
    "SimpleJSONStore", 
    "NullStore",
]
