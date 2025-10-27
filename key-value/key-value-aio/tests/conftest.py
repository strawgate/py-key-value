import asyncio
import logging
import os
import platform
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from time import sleep

import pytest
from docker import DockerClient
from docker.models.containers import Container

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


def get_docker_client() -> DockerClient:
    return DockerClient.from_env()


@pytest.fixture
def docker_client() -> DockerClient:
    return get_docker_client()


def docker_logs(name: str, print_logs: bool = False, raise_on_error: bool = False, log_level: int = logging.INFO) -> list[str]:
    client = get_docker_client()
    try:
        logs: list[str] = client.containers.get(name).logs().decode("utf-8").splitlines()

    except Exception:
        logger.info(f"Container {name} failed to get logs")
        if raise_on_error:
            raise
        return []

    if print_logs:
        logger.info(f"Container {name} logs:")
        for log in logs:
            logger.log(log_level, log)

    return logs


def docker_get(name: str, raise_on_not_found: bool = False) -> Container | None:
    from docker.errors import NotFound

    client = get_docker_client()
    try:
        return client.containers.get(name)
    except NotFound:
        logger.info(f"Container {name} failed to get")
        if raise_on_not_found:
            raise
        return None


def docker_pull(image: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Pulling image {image}")
    client = get_docker_client()
    try:
        client.images.pull(image)
    except Exception:
        logger.exception(f"Image {image} failed to pull")
        if raise_on_error:
            raise
        return False
    return True


def docker_stop(name: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Stopping container {name}")

    if not (container := docker_get(name=name, raise_on_not_found=False)):
        return False

    try:
        container.stop()
    except Exception:
        logger.info(f"Container {name} failed to stop")
        if raise_on_error:
            raise
        return False

    logger.info(f"Container {name} stopped")
    return True


def docker_wait_container_gone(name: str, max_tries: int = 10, wait_time: float = 1.0) -> bool:
    logger.info(f"Waiting for container {name} to be gone")
    count = 0
    while count < max_tries:
        if not docker_get(name=name, raise_on_not_found=False):
            return True
        sleep(wait_time)
        count += 1
    return False


def docker_rm(name: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Removing container {name}")

    if not (container := docker_get(name=name, raise_on_not_found=False)):
        return False

    try:
        container.remove()
    except Exception:
        logger.info(f"Container {name} failed to remove")
        if raise_on_error:
            raise
        return False
    logger.info(f"Container {name} removed")
    return True


def docker_run(name: str, image: str, ports: dict[str, int], environment: dict[str, str], raise_on_error: bool = False) -> bool:
    logger.info(f"Running container {name} with image {image} and ports {ports}")
    client = get_docker_client()
    try:
        client.containers.run(name=name, image=image, ports=ports, environment=environment, detach=True)
    except Exception:
        logger.exception(f"Container {name} failed to run")
        if raise_on_error:
            raise
        return False
    logger.info(f"Container {name} running")
    return True


@contextmanager
def docker_container(
    name: str, image: str, ports: dict[str, int], environment: dict[str, str] | None = None, raise_on_error: bool = True
) -> Iterator[None]:
    logger.info(f"Creating container {name} with image {image} and ports {ports}")
    try:
        docker_pull(image=image, raise_on_error=True)
        docker_stop(name=name, raise_on_error=False)
        docker_rm(name=name, raise_on_error=False)
        docker_wait_container_gone(name=name, max_tries=10, wait_time=1.0)
        docker_run(name=name, image=image, ports=ports, environment=environment or {}, raise_on_error=True)
        logger.info(f"Container {name} created")
        yield
        docker_logs(name, print_logs=True, raise_on_error=False)
    except Exception:
        logger.info(f"Creating container {name} failed")
        docker_logs(name, print_logs=True, raise_on_error=False, log_level=logging.ERROR)
        if raise_on_error:
            raise
        return
    finally:
        docker_stop(name, raise_on_error=False)
        docker_rm(name, raise_on_error=False)

    logger.info(f"Container {name} stopped and removed")
    return


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
