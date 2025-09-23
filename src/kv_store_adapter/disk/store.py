"""
Disk-based implementation of the KV Store protocol.
"""

import os
import json
import time
import threading
import fnmatch
import pickle
from pathlib import Path
from typing import Any, Optional, Union, List, Dict
from datetime import datetime, timedelta

from ..protocol import KVStoreProtocol
from ..exceptions import KeyNotFoundError


class DiskKVStore(KVStoreProtocol):
    """
    Disk-based implementation of KV Store protocol.
    
    This implementation stores data in the filesystem using JSON files for metadata
    and pickle files for the actual data. Each namespace is a directory, and each
    key is a file within that directory.
    """

    def __init__(self, base_path: Union[str, Path] = "./kv_store_data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        self._lock = threading.RLock()

    def _get_namespace_path(self, namespace: Optional[str]) -> Path:
        """Get the directory path for a namespace."""
        namespace_name = namespace or "default"
        return self.base_path / namespace_name

    def _get_key_paths(self, key: str, namespace: Optional[str]) -> tuple[Path, Path]:
        """Get the file paths for a key's data and metadata."""
        namespace_path = self._get_namespace_path(namespace)
        data_file = namespace_path / f"{key}.data"
        meta_file = namespace_path / f"{key}.meta"
        return data_file, meta_file

    def _is_expired(self, meta_file: Path) -> bool:
        """Check if a key is expired based on its metadata file."""
        if not meta_file.exists():
            return False
        
        try:
            with open(meta_file, 'r') as f:
                metadata = json.load(f)
            
            if 'expires_at' in metadata:
                return time.time() > metadata['expires_at']
        except (json.JSONDecodeError, KeyError, IOError):
            # If we can't read metadata, consider it not expired
            pass
        
        return False

    def _cleanup_expired_key(self, data_file: Path, meta_file: Path) -> None:
        """Remove expired key files."""
        try:
            if data_file.exists():
                data_file.unlink()
            if meta_file.exists():
                meta_file.unlink()
        except OSError:
            pass  # Ignore file deletion errors

    def _cleanup_expired_namespace(self, namespace_path: Path) -> None:
        """Clean up expired keys in a namespace."""
        if not namespace_path.exists():
            return
        
        for meta_file in namespace_path.glob("*.meta"):
            if self._is_expired(meta_file):
                key_name = meta_file.stem
                data_file = namespace_path / f"{key_name}.data"
                self._cleanup_expired_key(data_file, meta_file)

    def get(self, key: str, namespace: Optional[str] = None) -> Any:
        """Retrieve a value by key from the specified namespace."""
        with self._lock:
            data_file, meta_file = self._get_key_paths(key, namespace)
            
            # Check if expired and clean up
            if self._is_expired(meta_file):
                self._cleanup_expired_key(data_file, meta_file)
                raise KeyNotFoundError(f"Key '{key}' not found in namespace '{namespace or 'default'}'")
            
            if not data_file.exists():
                raise KeyNotFoundError(f"Key '{key}' not found in namespace '{namespace or 'default'}'")
            
            try:
                with open(data_file, 'rb') as f:
                    return pickle.load(f)
            except (IOError, pickle.PickleError) as e:
                raise KeyNotFoundError(f"Error reading key '{key}': {e}")

    def set(self, key: str, value: Any, namespace: Optional[str] = None, 
            ttl: Optional[Union[int, float, timedelta]] = None) -> None:
        """Store a key-value pair in the specified namespace."""
        with self._lock:
            namespace_path = self._get_namespace_path(namespace)
            namespace_path.mkdir(exist_ok=True)
            
            data_file, meta_file = self._get_key_paths(key, namespace)
            
            # Store the value
            try:
                with open(data_file, 'wb') as f:
                    pickle.dump(value, f)
            except (IOError, pickle.PickleError) as e:
                raise Exception(f"Error storing key '{key}': {e}")
            
            # Store metadata
            metadata = {
                'created_at': time.time(),
                'updated_at': time.time()
            }
            
            if ttl is not None:
                if isinstance(ttl, timedelta):
                    ttl_seconds = ttl.total_seconds()
                else:
                    ttl_seconds = float(ttl)
                
                metadata['expires_at'] = time.time() + ttl_seconds
            
            try:
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f)
            except (IOError, json.JSONEncodeError) as e:
                # If metadata write fails, clean up the data file
                if data_file.exists():
                    data_file.unlink()
                raise Exception(f"Error storing metadata for key '{key}': {e}")

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete a key from the specified namespace."""
        with self._lock:
            data_file, meta_file = self._get_key_paths(key, namespace)
            
            exists = data_file.exists() or meta_file.exists()
            
            # Remove both files if they exist
            try:
                if data_file.exists():
                    data_file.unlink()
                if meta_file.exists():
                    meta_file.unlink()
            except OSError:
                pass  # Ignore deletion errors
            
            return exists

    def ttl(self, key: str, namespace: Optional[str] = None) -> Optional[float]:
        """Get the time-to-live for a key in seconds."""
        with self._lock:
            if not self.exists(key, namespace):
                return None
            
            _, meta_file = self._get_key_paths(key, namespace)
            
            if not meta_file.exists():
                return None  # No TTL set
            
            try:
                with open(meta_file, 'r') as f:
                    metadata = json.load(f)
                
                if 'expires_at' not in metadata:
                    return None  # No TTL set
                
                remaining = metadata['expires_at'] - time.time()
                return max(0.0, remaining)
            except (json.JSONDecodeError, KeyError, IOError):
                return None

    def exists(self, key: str, namespace: Optional[str] = None) -> bool:
        """Check if a key exists in the specified namespace."""
        with self._lock:
            data_file, meta_file = self._get_key_paths(key, namespace)
            
            # Check if expired
            if self._is_expired(meta_file):
                self._cleanup_expired_key(data_file, meta_file)
                return False
            
            return data_file.exists()

    def keys(self, namespace: Optional[str] = None, pattern: str = "*") -> List[str]:
        """List keys in the specified namespace matching the pattern."""
        with self._lock:
            namespace_path = self._get_namespace_path(namespace)
            
            if not namespace_path.exists():
                return []
            
            # Clean up expired keys first
            self._cleanup_expired_namespace(namespace_path)
            
            # Get all data files
            all_keys = [f.stem for f in namespace_path.glob("*.data")]
            
            if pattern == "*":
                return all_keys
            
            return [key for key in all_keys if fnmatch.fnmatch(key, pattern)]

    def clear_namespace(self, namespace: str) -> int:
        """Clear all keys in a namespace."""
        with self._lock:
            namespace_path = self._get_namespace_path(namespace)
            
            if not namespace_path.exists():
                return 0
            
            count = 0
            # Remove all .data and .meta files
            for file_path in namespace_path.glob("*"):
                if file_path.is_file() and file_path.suffix in ['.data', '.meta']:
                    try:
                        file_path.unlink()
                        if file_path.suffix == '.data':
                            count += 1
                    except OSError:
                        pass  # Ignore deletion errors
            
            # Try to remove the directory if it's empty
            try:
                namespace_path.rmdir()
            except OSError:
                pass  # Directory not empty or other error
            
            return count

    def list_namespaces(self) -> List[str]:
        """List all available namespaces."""
        with self._lock:
            namespaces = []
            
            for item in self.base_path.iterdir():
                if item.is_dir():
                    # Clean up expired keys in the namespace
                    self._cleanup_expired_namespace(item)
                    
                    # Check if namespace has any data files
                    if any(item.glob("*.data")):
                        namespaces.append(item.name)
            
            return namespaces