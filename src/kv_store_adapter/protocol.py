"""
Protocol definition for KV Store implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Union, List
from datetime import datetime, timedelta


class KVStoreProtocol(ABC):
    """
    Abstract base class defining the interface for Key-Value store implementations.
    
    This protocol supports:
    - Basic operations: get, set, delete
    - TTL (Time To Live) functionality
    - Namespace/collection support for data organization
    """

    @abstractmethod
    def get(self, key: str, namespace: Optional[str] = None) -> Any:
        """
        Retrieve a value by key from the specified namespace.
        
        Args:
            key: The key to retrieve
            namespace: Optional namespace/collection name
            
        Returns:
            The value associated with the key
            
        Raises:
            KeyNotFoundError: If the key is not found
        """
        pass

    @abstractmethod
    def set(self, key: str, value: Any, namespace: Optional[str] = None, 
            ttl: Optional[Union[int, float, timedelta]] = None) -> None:
        """
        Store a key-value pair in the specified namespace.
        
        Args:
            key: The key to store
            value: The value to store
            namespace: Optional namespace/collection name
            ttl: Time to live in seconds (int/float) or timedelta object
        """
        pass

    @abstractmethod
    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """
        Delete a key from the specified namespace.
        
        Args:
            key: The key to delete
            namespace: Optional namespace/collection name
            
        Returns:
            True if the key was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
    def ttl(self, key: str, namespace: Optional[str] = None) -> Optional[float]:
        """
        Get the time-to-live for a key in seconds.
        
        Args:
            key: The key to check
            namespace: Optional namespace/collection name
            
        Returns:
            Remaining TTL in seconds, None if no TTL is set, or if key doesn't exist
        """
        pass

    @abstractmethod
    def exists(self, key: str, namespace: Optional[str] = None) -> bool:
        """
        Check if a key exists in the specified namespace.
        
        Args:
            key: The key to check
            namespace: Optional namespace/collection name
            
        Returns:
            True if the key exists, False otherwise
        """
        pass

    @abstractmethod
    def keys(self, namespace: Optional[str] = None, pattern: str = "*") -> List[str]:
        """
        List keys in the specified namespace matching the pattern.
        
        Args:
            namespace: Optional namespace/collection name
            pattern: Pattern to match keys against (supports * wildcard)
            
        Returns:
            List of matching keys
        """
        pass

    @abstractmethod
    def clear_namespace(self, namespace: str) -> int:
        """
        Clear all keys in a namespace.
        
        Args:
            namespace: The namespace to clear
            
        Returns:
            Number of keys deleted
        """
        pass

    @abstractmethod
    def list_namespaces(self) -> List[str]:
        """
        List all available namespaces.
        
        Returns:
            List of namespace names
        """
        pass