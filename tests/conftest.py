import asyncio
import logging
import os
import platform
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager

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
        asyncio.get_event_loop_policy().get_event_loop()
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
