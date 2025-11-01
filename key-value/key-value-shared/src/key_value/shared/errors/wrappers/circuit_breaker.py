from key_value.shared.errors.key_value import KeyValueOperationError


class CircuitOpenError(KeyValueOperationError):
    """Raised when the circuit breaker is open and requests are blocked."""

    def __init__(self, failure_count: int, last_failure_time: float | None = None):
        super().__init__(
            message="Circuit breaker is open. Requests are temporarily blocked due to consecutive failures.",
            extra_info={"failure_count": failure_count, "last_failure_time": last_failure_time},
        )
