import asyncio
import logging
import os
import platform
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)


@contextmanager
def try_import() -> Iterator[Callable[[], bool]]:
    import_success = False

    def check_import() -> bool:
        return import_success

    try:
        yield check_import
    except ImportError:
        pass
    else:
        import_success = True


def async_running_in_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def running_in_event_loop() -> bool:
    return False


def detect_docker() -> bool:
    try:
        result = subprocess.run(["docker", "ps"], check=False, capture_output=True, text=True)  # noqa: S607
    except Exception:
        return False
    else:
        return result.returncode == 0


def detect_on_ci() -> bool:
    return os.getenv("CI", "false") == "true"


def detect_on_windows() -> bool:
    return platform.system() == "Windows"


def detect_on_macos() -> bool:
    return platform.system() == "Darwin"


def detect_on_linux() -> bool:
    return platform.system() == "Linux"


def should_run_docker_tests() -> bool:
    if detect_on_ci():
        return all([detect_docker(), not detect_on_windows(), not detect_on_macos()])
    return detect_docker()


def should_skip_docker_tests() -> bool:
    return not should_run_docker_tests()


@contextmanager
def run_container_with_log_wait(container: Any, message: str, *, timeout: int | None = None) -> Iterator[Any]:
    """Start a testcontainer after configuring the newest available log wait API."""
    try:
        from testcontainers.core.wait_strategies import LogMessageWaitStrategy
    except ImportError:
        wait_strategy_configured = False
    else:
        wait_strategy = LogMessageWaitStrategy(message)
        if timeout is not None:
            wait_strategy = wait_strategy.with_startup_timeout(timeout)
        container.waiting_for(wait_strategy)
        wait_strategy_configured = True

    with container:
        if not wait_strategy_configured:
            from testcontainers.core.waiting_utils import wait_for_logs

            wait_for_logs(container, message, timeout=timeout or 120)
        yield container
