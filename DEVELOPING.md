# Developing

This monorepo contains two Python packages:

- `py-key-value-aio` (async; supported)
- `py-key-value-sync` (sync; generated from async)

## Prerequisites

- Python 3.10 (the sync codegen targets 3.10)
- `uv` for dependency management and running tools

## Setup

```bash
# From repo root
uv sync --all-extras --all-packages
```

## Lint and format

```bash
# From repo root
uv run ruff format .
uv run ruff check --fix .
```

## Test

```bash
# Async package tests
uv run pytest key-value/key-value-aio/tests -q

# Sync package tests (generated tests live under tests/code_gen)
uv run pytest key-value/key-value-sync/tests -q
```

## Generate/update sync package

The sync package is generated from the async package. After changes to the async code, regenerate the sync package:

```bash
uv run python scripts/build_sync_library.py
```

Notes:
- The codegen script lints the generated code automatically.
- Some extras differ between async and sync (e.g., valkey). Refer to each package’s README for current extras.

## Project layout

- Async package: `key-value/key-value-aio/`
- Sync package: `key-value/key-value-sync/`
- Codegen script: `scripts/build_sync_library.py`

## Releasing

TBD


