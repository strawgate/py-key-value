# AGENTS.md

This file provides guidelines and context for AI coding agents working on the
py-key-value project. For human developers, see [DEVELOPING.md](DEVELOPING.md).

## Development Workflow

### Required Pre-commit Checks

All four checks must pass before committing:

1. `make lint` - Runs Ruff formatting and linting (Python + Markdown)
2. `make typecheck` - Runs Basedpyright type checking
3. `make codegen` - Regenerates sync library from async
4. `make lint` - Runs Ruff formatting and linting after the other checks have

Or run all four together:

```bash
make precommit
```

### Testing Requirements

- All new features require tests in both async and sync packages
- Run `make test` to execute all test suites
- Run `make test-aio` for async package tests only
- Run `make test-sync` for sync package tests only
- Test coverage should be maintained

## Architecture

### Async-First Development

**This is a critical constraint**: Always modify the async package first.

- **Primary codebase**: `key-value/key-value-aio/` (async implementation)
- **Generated codebase**: `key-value/key-value-sync/` (DO NOT EDIT DIRECTLY)
- **Sync generation**: Run `make codegen` to generate sync from async

The sync library is automatically generated from the async library using
`scripts/build_sync_library.py`. All changes must be made to the async
package first, then regenerated into the sync package.

### Monorepo Structure

```text
key-value/
├── key-value-aio/         # Primary async library
├── key-value-sync/        # Generated sync library (DO NOT EDIT)
├── key-value-shared/      # Shared utilities and types
└── key-value-shared-test/ # Shared test utilities
scripts/
├── build_sync_library.py  # Codegen script for sync library
└── bump_versions.py       # Version management script
```

## Code Style & Conventions

### Python

- **Formatter/Linter**: Ruff (configured in `pyproject.toml`)
- **Line length**: 140 characters
- **Type checker**: Basedpyright (strict mode)
- **Runtime type checking**: Beartype (can be disabled via
  `PY_KEY_VALUE_DISABLE_BEARTYPE=true`)
- **Python version**: 3.10+ (sync codegen targets 3.10)

### Markdown

- **Linter**: markdownlint (`.markdownlint.jsonc`)
- **Line length**: 80 characters (excluding code blocks and tables)

## Common Pitfalls

### ManagedEntry Wrapper Objects

Raw values are **NEVER** stored directly in backends. The `ManagedEntry` wrapper
(from `key_value/shared/utils/managed_entry.py`) wraps values with metadata
like TTL and creation timestamp, typically serialized to/from JSON.

When implementing or debugging stores, remember that what's stored is not
the raw value but a `ManagedEntry` containing:

- The actual value
- Creation timestamp
- TTL metadata

### Python Version Compatibility

The sync codegen targets Python 3.10. Running the codegen script with a
different Python version may produce unexpected results or compatibility
issues. Use Python 3.10 when running `make codegen`.

### Optional Backend Dependencies

Store implementations have optional dependencies. Install extras as needed:

```bash
pip install py-key-value-aio[redis]      # Redis support
pip install py-key-value-aio[dynamodb]   # DynamoDB support
pip install py-key-value-aio[mongodb]    # MongoDB support
# etc. - see README.md for full list
```

### Sync Package is Generated

**Never edit files in `key-value/key-value-sync/` directly**. Any changes
will be overwritten when `make codegen` runs. Always make changes in the
async package and regenerate. Always run `make codegen` after making changes
to the async package. You will need to include the generated code in your pull
request. Nobody will generate it for you. This also means pull requests will contain
two copies of your changes, this is intentional!

## Make Commands Reference

| Command | Purpose |
|---------|---------|
| `make sync` | Install all dependencies |
| `make install` | Alias for `make sync` |
| `make lint` | Lint Python + Markdown |
| `make typecheck` | Run Basedpyright type checking |
| `make test` | Run all test suites |
| `make test-aio` | Run async package tests |
| `make test-sync` | Run sync package tests |
| `make test-shared` | Run shared package tests |
| `make codegen` | Generate sync library from async |
| `make precommit` | Run lint + typecheck + codegen |
| `make build` | Build all packages |

### Per-Project Commands

Add `PROJECT=<path>` to target a specific package:

```bash
make lint PROJECT=key-value/key-value-aio
make typecheck PROJECT=key-value/key-value-aio
make test PROJECT=key-value/key-value-aio
make build PROJECT=key-value/key-value-aio
```

## Key Protocols and Interfaces

### AsyncKeyValue Protocol

The core async interface is `AsyncKeyValue` protocol from
`key_value/aio/protocols/key_value.py`. All async stores implement this
protocol, which defines:

- `get`, `get_many` - Retrieve values
- `put`, `put_many` - Store values with optional TTL
- `delete`, `delete_many` - Remove values
- `ttl`, `ttl_many` - Get TTL information

### KeyValue Protocol (Sync)

The sync mirror is `KeyValue` from `key_value/sync/code_gen/protocols/key_value.py`,
generated from the async protocol.

## Store Implementations

Stores are located in:

- Async: `key-value/key-value-aio/src/key_value/aio/stores/`
- Sync: `key-value/key-value-sync/src/key_value/sync/code_gen/stores/`

Available backends include: DynamoDB, Elasticsearch, Memcached, Memory, Disk,
MongoDB, Redis, RocksDB, Valkey, Vault, Windows Registry, Keyring, and more.

## Wrappers

Wrappers add functionality to stores and are located in:

- Async: `key-value/key-value-aio/src/key_value/aio/wrappers/`
- Sync: `key-value/key-value-sync/src/key_value/sync/code_gen/wrappers/`

Wrappers include: Compression, DefaultValue, Encryption, Logging, Statistics,
Retry, Timeout, Cache, Prefix, TTL clamping, and more.

## Adapters

Adapters simplify store interactions but don't implement the protocol directly.
Located in:

- Async: `key-value/key-value-aio/src/key_value/aio/adapters/`
- Sync: `key-value/key-value-sync/src/key_value/sync/code_gen/adapters/`

Key adapters:

- `PydanticAdapter` - Type-safe Pydantic model storage
- `RaiseOnMissingAdapter` - Raise exceptions for missing keys

## Development Environment

### Option 1: DevContainer (Recommended)

The repository includes a DevContainer configuration for consistent development
environments. Open in VSCode and select "Reopen in Container" when prompted.

### Option 2: Local Development

Prerequisites:

- Python 3.10+
- `uv` for dependency management
- Node.js and npm for markdown linting

Setup:

```bash
make sync
```

## CI/CD

GitHub Actions workflows are in `.github/workflows/`:

- `test.yml` - Run tests across packages
  - `codegen_check` job - Ensures `make codegen lint` has been run before
    commits
  - `static_analysis` job - Runs linting and type checking per package
  - `test_quick` and `test_all` jobs - Run tests across Python versions and
    platforms
- `publish.yml` - Publish packages to PyPI
- `claude-on-mention.yml` - Claude Code assistant (can make PRs)
- `claude-on-open-label.yml` - Claude triage assistant (read-only analysis)

## Version Management

To bump versions across all packages:

```bash
make bump-version VERSION=1.2.3        # Actual bump
make bump-version-dry VERSION=1.2.3    # Dry run
```

## Getting Help

- For human developer documentation, see [DEVELOPING.md](DEVELOPING.md)
- For library usage documentation, see [README.md](README.md)
- For package-specific information, see READMEs in each package directory

## Radical Honesty

Agents should be honest! Properly document any problems encountered, share
feedback, and be transparent about your AI-assisted work.
