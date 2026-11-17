import asyncio
import contextlib
import importlib.util
import logging
import os
import platform
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

import pytest

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

# Some optional backends have no wheels for every supported platform or Python
# version (e.g. Windows, or a newly released Python), so the dev dependency group
# leaves them out there. Skip collecting their test modules when the backend
# package is not installed rather than failing at import time.
_OPTIONAL_BACKEND_TEST_MODULES = {
    "aerospike": "stores/aerospike/*",
    "chdb": "stores/chdb/*",
    "glide": "stores/valkey/*",
    "rocksdict": "stores/rocksdb/*",
}

collect_ignore_glob = [pattern for module, pattern in _OPTIONAL_BACKEND_TEST_MODULES.items() if importlib.util.find_spec(module) is None]


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

    try:
        container.start()
        if not wait_strategy_configured:
            from testcontainers.core.waiting_utils import wait_for_logs

            wait_for_logs(container, message, timeout=timeout or 120)
    except Exception:
        # The container is removed on stop, so capture its output first: without it a
        # container that exits during startup leaves no clue why in the CI log.
        _log_container_output(container)
        with contextlib.suppress(Exception):
            container.stop()
        raise

    try:
        yield container
    finally:
        container.stop()


def _log_container_output(container: Any, max_chars: int = 20_000) -> None:
    try:
        stdout, stderr = container.get_logs()
    except Exception:
        logger.exception("Container failed to start and its logs could not be read")
        return
    output = (stdout + stderr).decode(errors="replace")
    logger.error("Container %s failed to start. Last output:\n%s", getattr(container, "image", "?"), output[-max_chars:])


class UnavailableAWSHandler(BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:
        self.send_error(503)

    def do_POST(self) -> None:
        self.send_error(503)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture
def unavailable_aws_endpoint() -> Iterator[str]:
    """Serve a deterministic error while reserving the endpoint for the test."""
    with ThreadingHTTPServer(("127.0.0.1", 0), UnavailableAWSHandler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join(timeout=5)
