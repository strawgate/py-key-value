"""
In-memory implementation of the KV Store protocol.
"""

import time
import threading
import fnmatch
from typing import Any, Optional, Union, List, Dict, Tuple
from datetime import datetime, timedelta

from ..protocol import KVStoreProtocol
from ..exceptions import KeyNotFoundError


class MemoryKVStore(KVStoreProtocol):
    """
    In-memory implementation of KV Store protocol.
    
    This implementation stores all data in memory using Python dictionaries.
    TTL is implemented using expiration timestamps.
    Thread-safe operations are ensured using locks.
    """

    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] = {}  # namespace -> key -> value
        self._ttl: Dict[str, Dict[str, float]] = {}  # namespace -> key -> expiration_time
        self._lock = threading.RLock()

    def _get_namespace_key(self, namespace: Optional[str]) -> str:
        """Get the internal namespace key, defaulting to 'default'."""
        return namespace or "default"

    def _cleanup_expired(self, namespace_key: str) -> None:
        """Remove expired keys from a namespace."""
        if namespace_key not in self._ttl:
            return
        
        current_time = time.time()
        expired_keys = [
            key for key, exp_time in self._ttl[namespace_key].items()
            if exp_time <= current_time
        ]
        
        for key in expired_keys:
            if namespace_key in self._data and key in self._data[namespace_key]:
                del self._data[namespace_key][key]
            del self._ttl[namespace_key][key]

    def _is_expired(self, key: str, namespace_key: str) -> bool:
        """Check if a key is expired."""
        if namespace_key not in self._ttl or key not in self._ttl[namespace_key]:
            return False
        return self._ttl[namespace_key][key] <= time.time()

    def get(self, key: str, namespace: Optional[str] = None) -> Any:
        """Retrieve a value by key from the specified namespace."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            self._cleanup_expired(namespace_key)
            
            if (namespace_key not in self._data or 
                key not in self._data[namespace_key] or
                self._is_expired(key, namespace_key)):
                raise KeyNotFoundError(f"Key '{key}' not found in namespace '{namespace_key}'")
            
            return self._data[namespace_key][key]

    def set(self, key: str, value: Any, namespace: Optional[str] = None, 
            ttl: Optional[Union[int, float, timedelta]] = None) -> None:
        """Store a key-value pair in the specified namespace."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            # Initialize namespace if it doesn't exist
            if namespace_key not in self._data:
                self._data[namespace_key] = {}
            if namespace_key not in self._ttl:
                self._ttl[namespace_key] = {}
            
            # Store the value
            self._data[namespace_key][key] = value
            
            # Handle TTL
            if ttl is not None:
                if isinstance(ttl, timedelta):
                    ttl_seconds = ttl.total_seconds()
                else:
                    ttl_seconds = float(ttl)
                
                self._ttl[namespace_key][key] = time.time() + ttl_seconds
            else:
                # Remove any existing TTL
                if key in self._ttl[namespace_key]:
                    del self._ttl[namespace_key][key]

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete a key from the specified namespace."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            self._cleanup_expired(namespace_key)
            
            if (namespace_key not in self._data or 
                key not in self._data[namespace_key]):
                return False
            
            del self._data[namespace_key][key]
            
            # Remove TTL if it exists
            if namespace_key in self._ttl and key in self._ttl[namespace_key]:
                del self._ttl[namespace_key][key]
            
            return True

    def ttl(self, key: str, namespace: Optional[str] = None) -> Optional[float]:
        """Get the time-to-live for a key in seconds."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            if not self.exists(key, namespace):
                return None
            
            if (namespace_key not in self._ttl or 
                key not in self._ttl[namespace_key]):
                return None  # No TTL set
            
            remaining = self._ttl[namespace_key][key] - time.time()
            return max(0.0, remaining)

    def exists(self, key: str, namespace: Optional[str] = None) -> bool:
        """Check if a key exists in the specified namespace."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            self._cleanup_expired(namespace_key)
            
            return (namespace_key in self._data and 
                    key in self._data[namespace_key] and
                    not self._is_expired(key, namespace_key))

    def keys(self, namespace: Optional[str] = None, pattern: str = "*") -> List[str]:
        """List keys in the specified namespace matching the pattern."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            self._cleanup_expired(namespace_key)
            
            if namespace_key not in self._data:
                return []
            
            all_keys = list(self._data[namespace_key].keys())
            
            if pattern == "*":
                return all_keys
            
            return [key for key in all_keys if fnmatch.fnmatch(key, pattern)]

    def clear_namespace(self, namespace: str) -> int:
        """Clear all keys in a namespace."""
        namespace_key = self._get_namespace_key(namespace)
        
        with self._lock:
            if namespace_key not in self._data:
                return 0
            
            count = len(self._data[namespace_key])
            self._data[namespace_key].clear()
            
            if namespace_key in self._ttl:
                self._ttl[namespace_key].clear()
            
            return count

    def list_namespaces(self) -> List[str]:
        """List all available namespaces."""
        with self._lock:
            # Clean up expired keys first
            for ns in list(self._data.keys()):
                self._cleanup_expired(ns)
            
            # Return namespaces that have data
            return [ns for ns in self._data.keys() if self._data[ns]]