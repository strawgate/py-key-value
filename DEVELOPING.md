# Developing

This monorepo contains two Python packages:

- `py-key-value-aio` (async; supported)
- `py-key-value-sync` (sync; generated from async)

## Prerequisites

### Option 1: DevContainer (Recommended)

- Docker Desktop or compatible container runtime
- Visual Studio Code with the Dev Containers extension
- Open the repository in VSCode and select "Reopen in Container" when prompted

### Option 2: Local Development

- Python 3.10 (the sync codegen targets 3.10)
- `uv` for dependency management and running tools
- Node.js and npm for markdown linting

## Setup

```bash
make sync
```

## Common Commands

Run `make help` to see all available targets. The Makefile supports both
whole-repo and per-project operations.

### Lint and Format

```bash
# Lint everything (Python + Markdown)
make lint

# Lint a specific project
make lint PROJECT=key-value/key-value-aio
```

### Type Checking

```bash
# Type check everything
make typecheck

# Type check a specific project
make typecheck PROJECT=key-value/key-value-aio
```

### Testing

```bash
# Run all tests
make test

# Run tests for a specific project
make test PROJECT=key-value/key-value-aio

# Convenience targets for specific packages
make test-aio      # async package
make test-sync     # sync package
make test-shared   # shared package
```

### Building

```bash
# Build all packages
make build

# Build a specific project
make build PROJECT=key-value/key-value-aio
```

## Generate/Update Sync Package

The sync package is generated from the async package. After changes to the
async code, regenerate the sync package:

```bash
make codegen
```

Notes:

- The codegen script lints the generated code automatically.
- Some extras differ between async and sync (e.g., valkey). Refer to each
  package's README for current extras.

## Pre-commit Checks

Before committing, run:

```bash
make precommit
```

This runs linting, type checking, and code generation.

## Using Makefile in CI

The Makefile targets support per-project operations, making them
suitable for CI workflows:

```yaml
# Example: CI workflow step
- name: "Lint"
  run: make lint PROJECT=${{ matrix.project }}

- name: "Type Check"
  run: make typecheck PROJECT=${{ matrix.project }}

- name: "Test"
  run: make test PROJECT=${{ matrix.project }}
```

## Project Layout

- Async package: `key-value/key-value-aio/`
- Sync package: `key-value/key-value-sync/`
- Shared utilities: `key-value/key-value-shared/`
- Codegen script: `scripts/build_sync_library.py`
- Makefile: Root directory

## Releasing

TBD
