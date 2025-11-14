from key_value.shared.errors.key_value import KeyValueOperationError


class BulkheadFullError(KeyValueOperationError):
    """Raised when the bulkhead is full and cannot accept more concurrent operations."""

    def __init__(self, max_concurrent: int, max_waiting: int):
        super().__init__(
            message="Bulkhead is full. Maximum concurrent operations and waiting queue limit reached.",
            extra_info={"max_concurrent": max_concurrent, "max_waiting": max_waiting},
        )
