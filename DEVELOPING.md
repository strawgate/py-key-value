# Developing

This monorepo contains two Python packages:

- `py-key-value-aio` (async; supported)
- `py-key-value-sync` (sync; generated from async)

## Prerequisites

### Option 1: DevContainer (Recommended)

- Docker Desktop or compatible container runtime
- Visual Studio Code with the Dev Containers extension
- Open the repository in VSCode and select "Reopen in Container" when prompted

The DevContainer provides a pre-configured development environment with all dependencies installed automatically.

### Option 2: Local Development

- Python 3.10 (the sync codegen targets 3.10)
- `uv` for dependency management and running tools
- Node.js and npm for markdown linting

## Setup

```bash
# From repo root
uv sync --all-extras --all-packages

# Install Node.js dependencies for markdown linting
npm install
```

## Lint and format

```bash
# From repo root - Lint Python code
uv run ruff format .
uv run ruff check --fix .

# Lint markdown files
npm run lint:md

# Or use the Makefile to run all linters
make lint
```

## Test

```bash
# Async package tests
uv run pytest key-value/key-value-aio/tests -q

# Sync package tests (generated tests live under tests/code_gen)
uv run pytest key-value/key-value-sync/tests -q
```

## Generate/update sync package

The sync package is generated from the async package. After changes to the
async code, regenerate the sync package:

```bash
uv run python scripts/build_sync_library.py
```

Notes:

- The codegen script lints the generated code automatically.
- Some extras differ between async and sync (e.g., valkey). Refer to each
  package's README for current extras.

## Project layout

- Async package: `key-value/key-value-aio/`
- Sync package: `key-value/key-value-sync/`
- Codegen script: `scripts/build_sync_library.py`

## Releasing

TBD
