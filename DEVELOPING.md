# Developing

This repository contains the `py-key-value-aio` async key-value library.

## Prerequisites

### Option 1: DevContainer (Recommended)

- Docker Desktop or compatible container runtime
- Visual Studio Code with the Dev Containers extension
- Open the repository in VSCode and select "Reopen in Container" when prompted

### Option 2: Local Development

- Python 3.10+
- `uv` for dependency management and running tools
- Node.js and npm for markdown linting

## Setup

```bash
make sync
```

## Make Commands Reference

Run `make help` to see all available targets.

| Command | Purpose |
| ------- | ------- |
| `make sync` | Install all dependencies |
| `make install` | Alias for `make sync` |
| `make lint` | Lint Python + Markdown |
| `make typecheck` | Run Basedpyright type checking |
| `make test` | Run all tests (verbose) |
| `make test-concise` | Run all tests (concise output) |
| `make precommit` | Run lint + typecheck |
| `make build` | Build package |

## Pre-commit Checks

Before committing, run:

```bash
make precommit
```

This runs linting and type checking.

## Project Structure

```text
src/
└── key_value/
    ├── aio/               # Async key-value library
    │   ├── adapters/      # Type adapters (Pydantic, Dataclass, etc.)
    │   ├── protocols/     # Protocol definitions
    │   ├── stores/        # Backend implementations
    │   ├── wrappers/      # Store wrappers
    │   ├── _utils/        # Shared utilities (ManagedEntry, TTL, etc.)
    │   └── errors/        # Shared error classes
tests/                     # Test suite
scripts/
└── bump_versions.py       # Version management script
```

## Key Protocols and Interfaces

### AsyncKeyValue Protocol

The core interface is `AsyncKeyValue` protocol from
`key_value.aio.protocols.key_value`. All stores implement this protocol,
which defines:

- `get`, `get_many` - Retrieve values
- `put`, `put_many` - Store values with optional TTL
- `delete`, `delete_many` - Remove values
- `ttl`, `ttl_many` - Get TTL information

### Store Implementations

Stores are located in `src/key_value/aio/stores/`.

Available backends include: DynamoDB, Elasticsearch, Firestore, Memcached,
Memory, Disk, MongoDB, Redis, RocksDB, Valkey, Vault, Windows Registry,
Keyring, and more.

### Wrappers

Wrappers add functionality to stores and are located in
`src/key_value/aio/wrappers/`.

Wrappers include: Compression, DefaultValue, Encryption, Logging, Statistics,
Retry, Timeout, Cache, Prefix, TTL clamping, and more.

### Adapters

Adapters simplify store interactions but don't implement the protocol directly.
Located in `src/key_value/aio/adapters/`.

Key adapters:

- `PydanticAdapter` - Type-safe Pydantic model storage
- `RaiseOnMissingAdapter` - Raise exceptions for missing keys

## Optional Backend Dependencies

Store implementations have optional dependencies. Install extras as needed:

```bash
pip install py-key-value-aio[redis]      # Redis support
pip install py-key-value-aio[dynamodb]   # DynamoDB support
pip install py-key-value-aio[mongodb]    # MongoDB support
# etc. - see README.md for full list
```

## CI/CD

GitHub Actions workflows are in `.github/workflows/`:

- `test.yml` - Run tests across Python versions and platforms
- `publish.yml` - Publish package to PyPI
- `claude-mention-pr.yml` - Claude Code assistant (can make PRs)
- `claude-triage.yml` - Claude triage assistant (read-only analysis)
- `claude-test-failure.yml` - Claude test failure analysis

## Version Management

To bump version:

```bash
make bump-version VERSION=1.2.3        # Actual bump
make bump-version-dry VERSION=1.2.3    # Dry run
```

## Releasing

TBD
