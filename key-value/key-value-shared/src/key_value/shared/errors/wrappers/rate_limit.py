from key_value.shared.errors.key_value import KeyValueOperationError


class RateLimitExceededError(KeyValueOperationError):
    """Raised when the rate limit has been exceeded."""

    def __init__(self, current_requests: int, max_requests: int, window_seconds: float):
        super().__init__(
            message="Rate limit exceeded. Too many requests within the time window.",
            extra_info={"current_requests": current_requests, "max_requests": max_requests, "window_seconds": window_seconds},
        )
