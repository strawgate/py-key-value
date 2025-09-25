## Architecture Overview
The `py-kv-store-adapter` project provides a pluggable, async-first interface for various key-value (KV) store backends in Python. Its core purpose is to abstract away the underlying KV store implementation, offering a consistent `KVStoreProtocol` for interacting with different storage solutions like Redis, Elasticsearch, in-memory caches, and disk-based stores. The architecture distinguishes between two main store types: `Unmanaged Stores` (`BaseKVStore`) which handle their own TTL and collection management, and `Managed Stores` (`BaseManagedKVStore`) which automatically manage `ManagedEntry` objects for TTL and expiration. The system also supports `Adapters` for transforming data (e.g., Pydantic models) and `Wrappers` for adding cross-cutting concerns like statistics tracking or TTL clamping, which can be chained. Key concepts include collections for namespacing, compound keys for internal storage, and automatic TTL management.

## Code Style & Conventions
- **Python Version**: Requires Python 3.10 or higher (`pyproject.toml:project.requires-python`).
- **Dependency Management**: Uses `uv` for dependency management (`DEVELOPING.md:L10`). Development dependencies are managed via `uv sync --group dev` (`DEVELOPING.md:L24`).
- **Linting & Formatting**: Enforced by Ruff (`pyproject.toml:[tool.ruff]`).
  - Line length: 140 characters (`pyproject.toml:line-length`).
  - Fixable issues: All auto-fixable issues are configured to be fixed (`pyproject.toml:lint.fixable`).
  - Ignored rules: `COM812`, `PLR0913` (too many arguments) (`pyproject.toml:lint.ignore`).
  - Extended select rules: A broad range of linting rules are enabled, including `A`, `ARG`, `B`, `C4`, `COM`, `DTZ`, `E`, `EM`, `F`, `FURB`, `I`, `LOG`, `N`, `PERF`, `PIE`, `PLR`, `PLW`, `PT`, `PTH`, `Q`, `RET`, `RSE`, `RUF`, `S`, `SIM`, `TC`, `TID`, `TRY`, `UP`, `W` (`pyproject.toml:lint.extend-select`).
  - Per-file ignores: Test files (`**/tests/*.py`) ignore `S101` (asserts), `DTZ005` (datetime.UTC), `PLR2004` (magic values), `E501` (line length) (`pyproject.toml:[tool.ruff.lint.extend-per-file-ignores]`).
- **Type Checking**: Uses Pyright (`pyproject.toml:[tool.pyright]`).
  - Python version: 3.10 (`pyproject.toml:pythonVersion`).
  - Type checking mode: `recommended` (`pyproject.toml:typeCheckingMode`).
  - `src/` directory is included for type checking (`pyproject.toml:include`).
  - Missing type stubs, explicit `Any`, and missing module sources are not reported (`pyproject.toml:reportMissingTypeStubs`, `reportExplicitAny`, `reportMissingModuleSource`).

## Quick Recipes
| Command | Description |
|---|---|
| Install dependencies | `uv sync --group dev` (`DEVELOPING.md:L24`) |
| Run all tests | `uv run pytest` (`DEVELOPING.md:L169`) |
| Run tests with coverage | `uv run pytest --cov=src/kv_store_adapter --cov-report=html` (`DEVELOPING.md:L172`) |
| Run specific test file | `uv run pytest tests/stores/redis/test_redis.py` (`DEVELOPING.md:L175`) |
| Check code style (lint) | `uv run ruff check` (`DEVELOPING.md:L277`) |
| Fix auto-fixable lint issues | `uv run ruff check --fix` (`DEVELOPING.md:L280`) |
| Format code | `uv run ruff format` (`DEVELOPING.md:L283`) |
| Type check | `pyright` (`DEVELOPING.md:L292`) |
| Start external services for integration tests | `docker-compose up -d` (`DEVELOPING.md:L187`) |
| Stop external services | `docker-compose down` (`DEVELOPING.md:L193`) |

## Dependencies & Compatibility
- **Critical Runtime Dependencies**:
    - `cachetools>=6.0.0` for `MemoryStore` (`pyproject.toml:L26`).
    - `diskcache>=5.6.0` for `DiskStore` (`pyproject.toml:L27`).
    - `redis>=6.0.0` for `RedisStore` (`pyproject.toml:L28`).
    - `elasticsearch>=9.0.0`, `aiohttp>=3.12` for `ElasticsearchStore` (`pyproject.toml:L29`).
    - `pydantic>=2.11.9` for `PydanticAdapter` (`pyproject.toml:L30`).
- **Toolchain & Versions**:
    - Python: `>=3.10` (`pyproject.toml:L6`).
    - `uv`: Used for dependency management and running commands (`DEVELOPING.md:L10`).
    - `pytest`: Test runner (`pyproject.toml:L45`). `asyncio_mode = \"auto\"` is configured for async tests (`pyproject.toml:L33`).
    - `ruff`: Linter and formatter (`pyproject.toml:L48`).
    - `basedpyright`: Type checker (`pyproject.toml:L54`).
- **Observability**:
    - The `StatisticsWrapper` (`src/kv_store_adapter/stores/wrappers/statistics.py`) provides in-memory tracking of operation counts, hits, and misses for `get`, `put`, `delete`, `exists`, `keys`, and `clear_collection` operations per collection. It can be enabled during initialization.

## Unique Workflows
- **Adding New Store Implementations**: Developers can extend the system by creating new store classes that inherit from either `BaseKVStore` (unmanaged) or `BaseManagedKVStore` (managed), implementing abstract methods for `get`, `put`, `delete`, `ttl`, `exists`, `keys`, `clear_collection`, and `list_collections` (`DEVELOPING.md:L298-L349`).
- **Wrapper/Adapter Chaining**: The design allows for chaining multiple wrappers and adapters to compose complex behaviors, such as `PydanticAdapter(SingleCollectionWrapper(store, \"users\"), User)` (`README.md:L174`).
- **CI/CD**: GitHub Actions workflows (`.github/workflows/`) are configured to run tests, linting, type checking, and formatting on pull requests and pushes to `main` (`.github/workflows/test_pull_request.yml`). A separate workflow handles publishing to PyPI on release creation (`.github/workflows/publish-py-kv-store-adapter.yml`).

## API Surface Map
The primary API surface is defined by the `KVStoreProtocol` (`src/kv_store_adapter/types.py:L26-L44`) and extended by `BaseKVStore` (`src/kv_store_adapter/stores/base/unmanaged.py:L11-L76`) and `BaseManagedKVStore` (`src/kv_store_adapter/stores/base/managed.py:L21-L122`).
- **Core KV Operations**: `get(collection, key)`, `put(collection, key, value, ttl)`, `delete(collection, key)`, `exists(collection, key)`, `ttl(collection, key)`.
- **Management Operations (BaseKVStore)**: `keys(collection)`, `clear_collection(collection)`, `list_collections()`, `cull()`.


## Onboarding Steps
- **Understand Core Concepts**: Familiarize yourself with `KVStoreProtocol`, `BaseKVStore`, `BaseManagedKVStore`, `ManagedEntry`, `Collections`, `Compound Keys`, `TTL Management`, `Wrappers`, and `Adapters` by reading `README.md` and `DEVELOPING.md`.
- **Development Setup**: Follow the \"Development Setup\" in `DEVELOPING.md` to clone the repository, install `uv`, sync dependencies (`uv sync --group dev`), activate the virtual environment, and install pre-commit hooks.
- **Testing**: Review `DEVELOPING.md`'s \"Testing\" section for how to run tests, set up test environments using Docker Compose, and write new tests using `BaseKVStoreTestCase` or `BaseManagedKVStoreTestCase` from `tests/cases.py`.
- **Code Quality**: Understand the `ruff` and `pyright` configurations in `pyproject.toml` and how to run them (`uv run ruff check`, `uv run ruff format`, `pyright`).
- **Adding New Stores**: If extending the project, follow the \"Adding New Store Implementations\" guide in `DEVELOPING.md` for detailed steps on choosing a base class, creating the store, structuring the package, and adding tests.

## Getting Unstuck
- **General Development Issues**: Refer to the \"Development Guide\" in [`DEVELOPING.md`](DEVELOPING.md) for setup, testing, and contribution guidelines.
- **Integration Tests with External Services**: If integration tests fail, ensure Docker and Docker Compose are running and the necessary services (Redis, Elasticsearch) are started via `docker-compose up -d` as described in [`DEVELOPING.md:L181-L194`](DEVELOPING.md:L181-L194). Check `.env` file configuration for external services (`DEVELOPING.md:L197-L211`).
- **Redis Test Failures**: The `tests/stores/redis/test_redis.py` fixture `setup_redis` attempts to manage a Dockerized Redis instance. If Redis fails to start, check Docker logs or manually ensure the `redis-test` container is running and accessible.
- **SingleCollectionWrapper Limitations**: This wrapper does not support `clear_collection` or `list_collections` operations, raising `NotImplementedError` if called (`src/kv_store_adapter/stores/wrappers/single_collection.py:L47-L64`