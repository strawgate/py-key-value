"""
Redis-based implementation of the KV Store protocol.
"""

import pickle
import fnmatch
from typing import Any, Optional, Union, List
from datetime import timedelta

from ..protocol import KVStoreProtocol
from ..exceptions import KeyNotFoundError

try:
    import redis
except ImportError:
    redis = None


class RedisKVStore(KVStoreProtocol):
    """
    Redis-based implementation of KV Store protocol.
    
    This implementation uses Redis as the backend storage.
    Namespaces are implemented using key prefixes.
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, 
                 password: Optional[str] = None, **redis_kwargs):
        if redis is None:
            raise ImportError("redis package is required for RedisKVStore. Install with: pip install redis")
        
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            db=db, 
            password=password,
            decode_responses=False,  # We'll handle encoding ourselves
            **redis_kwargs
        )
        
        # Test connection
        try:
            self.redis_client.ping()
        except redis.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")

    def _get_redis_key(self, key: str, namespace: Optional[str]) -> str:
        """Get the Redis key with namespace prefix."""
        namespace_prefix = f"{namespace or 'default'}:"
        return f"{namespace_prefix}{key}"

    def _get_namespace_prefix(self, namespace: Optional[str]) -> str:
        """Get the namespace prefix for pattern matching."""
        return f"{namespace or 'default'}:*"

    def _strip_namespace_from_key(self, redis_key: str, namespace: Optional[str]) -> str:
        """Remove namespace prefix from Redis key to get the original key."""
        namespace_prefix = f"{namespace or 'default'}:"
        if redis_key.startswith(namespace_prefix):
            return redis_key[len(namespace_prefix):]
        return redis_key

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize a value for storage in Redis."""
        return pickle.dumps(value)

    def _deserialize_value(self, data: bytes) -> Any:
        """Deserialize a value from Redis storage."""
        return pickle.loads(data)

    def get(self, key: str, namespace: Optional[str] = None) -> Any:
        """Retrieve a value by key from the specified namespace."""
        redis_key = self._get_redis_key(key, namespace)
        
        data = self.redis_client.get(redis_key)
        if data is None:
            raise KeyNotFoundError(f"Key '{key}' not found in namespace '{namespace or 'default'}'")
        
        return self._deserialize_value(data)

    def set(self, key: str, value: Any, namespace: Optional[str] = None, 
            ttl: Optional[Union[int, float, timedelta]] = None) -> None:
        """Store a key-value pair in the specified namespace."""
        redis_key = self._get_redis_key(key, namespace)
        serialized_value = self._serialize_value(value)
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl_seconds = ttl.total_seconds()
            else:
                ttl_seconds = float(ttl)
            
            # Redis expects integer seconds for ex parameter
            self.redis_client.setex(redis_key, int(ttl_seconds), serialized_value)
        else:
            self.redis_client.set(redis_key, serialized_value)

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete a key from the specified namespace."""
        redis_key = self._get_redis_key(key, namespace)
        result = self.redis_client.delete(redis_key)
        return result > 0

    def ttl(self, key: str, namespace: Optional[str] = None) -> Optional[float]:
        """Get the time-to-live for a key in seconds."""
        redis_key = self._get_redis_key(key, namespace)
        
        if not self.redis_client.exists(redis_key):
            return None
        
        ttl_seconds = self.redis_client.ttl(redis_key)
        
        if ttl_seconds == -1:
            return None  # No TTL set
        elif ttl_seconds == -2:
            return None  # Key doesn't exist (shouldn't happen due to exists check)
        else:
            return float(ttl_seconds)

    def exists(self, key: str, namespace: Optional[str] = None) -> bool:
        """Check if a key exists in the specified namespace."""
        redis_key = self._get_redis_key(key, namespace)
        return bool(self.redis_client.exists(redis_key))

    def keys(self, namespace: Optional[str] = None, pattern: str = "*") -> List[str]:
        """List keys in the specified namespace matching the pattern."""
        # Get all keys in the namespace
        namespace_pattern = self._get_namespace_prefix(namespace)
        redis_keys = self.redis_client.keys(namespace_pattern)
        
        # Strip namespace prefix and filter by pattern
        original_keys = [
            self._strip_namespace_from_key(key.decode('utf-8'), namespace) 
            for key in redis_keys
        ]
        
        if pattern == "*":
            return original_keys
        
        return [key for key in original_keys if fnmatch.fnmatch(key, pattern)]

    def clear_namespace(self, namespace: str) -> int:
        """Clear all keys in a namespace."""
        namespace_pattern = self._get_namespace_prefix(namespace)
        keys_to_delete = self.redis_client.keys(namespace_pattern)
        
        if not keys_to_delete:
            return 0
        
        return self.redis_client.delete(*keys_to_delete)

    def list_namespaces(self) -> List[str]:
        """List all available namespaces."""
        # Get all keys and extract unique namespace prefixes
        all_keys = self.redis_client.keys("*")
        namespaces = set()
        
        for key in all_keys:
            key_str = key.decode('utf-8')
            if ':' in key_str:
                namespace = key_str.split(':', 1)[0]
                namespaces.add(namespace)
        
        return list(namespaces)