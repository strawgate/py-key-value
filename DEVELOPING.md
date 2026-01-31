# Developing

This monorepo contains the Python package:

- `py-key-value-aio` (async)

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
make test-shared   # shared package
```

### Building

```bash
# Build all packages
make build

# Build a specific project
make build PROJECT=key-value/key-value-aio
```

## Pre-commit Checks

Before committing, run:

```bash
make precommit
```

This runs linting and type checking.

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
- Shared utilities: `key-value/key-value-shared/`
- Makefile: Root directory

## Releasing

TBD
