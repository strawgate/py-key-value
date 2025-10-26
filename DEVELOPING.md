# Developing

This monorepo contains two Python packages:

- `py-key-value-aio` (async; supported)
- `py-key-value-sync` (sync; generated from async)

## Prerequisites

### Option 1: DevContainer (Recommended)

- Docker Desktop or compatible container runtime
- Visual Studio Code with the Dev Containers extension
- Open the repository in VSCode and select "Reopen in Container" when prompted

All dependencies will be installed automatically when the container is created.

### Option 2: Local Development

- Python 3.10 (the sync codegen targets 3.10)
- `uv` for dependency management and running tools
- Node.js and npm for markdown linting

## Setup

```bash
# From repo root - using Makefile
make setup

# Or manually
uv sync --all-extras --all-packages
npm install
```

## Lint and format

```bash
# Using Makefile (recommended)
make lint

# Or manually
uv run ruff format .
uv run ruff check --fix .
npm run lint:md
```

## Test

```bash
# Using Makefile (recommended)
make test          # Run all tests
make test-aio      # Run async tests only
make test-sync     # Run sync tests only

# Or manually
uv run pytest key-value/key-value-aio/tests -vv
uv run pytest key-value/key-value-sync/tests -vv
```

## Generate/update sync package

The sync package is generated from the async package. After changes to the
async code, regenerate the sync package:

```bash
# Using Makefile (recommended)
make codegen

# Or manually
uv run python scripts/build_sync_library.py
```

Notes:

- The codegen script lints the generated code automatically.
- Some extras differ between async and sync (e.g., valkey). Refer to each
  package's README for current extras.

## Pre-commit checks

Run all pre-commit checks (lint, typecheck, and codegen):

```bash
make precommit
```

## Project layout

- Async package: `key-value/key-value-aio/`
- Sync package: `key-value/key-value-sync/`
- Codegen script: `scripts/build_sync_library.py`

## Releasing

TBD
