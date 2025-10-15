import asyncio
import logging
import os
import platform
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest
from docker import DockerClient

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


@pytest.fixture(scope="session")
def docker_client() -> DockerClient:
    return get_docker_client()


def docker_logs(name: str, print_logs: bool = False) -> list[str]:
    client = get_docker_client()
    logs: list[str] = client.containers.get(name).logs().decode("utf-8").splitlines()

    if print_logs:
        logger.info(f"Container {name} logs:")
        for log in logs:
            logger.info(log)

    return logs


def docker_pull(image: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Pulling image {image}")
    client = get_docker_client()
    try:
        client.images.pull(image)
    except Exception:
        logger.info(f"Image {image} failed to pull")
        if raise_on_error:
            raise
        return False
    return True


def docker_stop(name: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Stopping container {name}")
    client = get_docker_client()
    try:
        client.containers.get(name).stop()
    except Exception:
        logger.info(f"Container {name} failed to stop")
        if raise_on_error:
            raise
        return False
    logger.info(f"Container {name} stopped")
    return True


def docker_rm(name: str, raise_on_error: bool = False) -> bool:
    logger.info(f"Removing container {name}")
    client = get_docker_client()
    try:
        client.containers.get(container_id=name).remove()
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
        logger.info(f"Container {name} failed to run")
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
        docker_run(name=name, image=image, ports=ports, environment=environment or {}, raise_on_error=True)
        logger.info(f"Container {name} created")
        yield
    except Exception:
        logger.info(f"Container {name} failed to create")
        if raise_on_error:
            raise
        return
    finally:
        docker_stop(name, raise_on_error=False)
        docker_logs(name, print_logs=True)
        docker_rm(name, raise_on_error=False)

    logger.info(f"Container {name} stopped and removed")
    return


def async_running_in_event_loop() -> bool:
    try:
        asyncio.get_event_loop()
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
