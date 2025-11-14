"""PostgreSQL store for py-key-value-aio."""

try:
    from key_value.aio.stores.postgresql.store import PostgreSQLStore
except ImportError as e:
    msg = 'PostgreSQLStore requires the "postgresql" extra. Install via: pip install "py-key-value-aio[postgresql]"'
    raise ImportError(msg) from e

__all__ = ["PostgreSQLStore"]
