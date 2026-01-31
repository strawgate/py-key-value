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

## Common Commands

Run `make help` to see all available targets.

### Lint and Format

```bash
# Lint everything (Python + Markdown)
make lint
```

### Type Checking

```bash
# Type check everything
make typecheck
```

### Testing

```bash
# Run all tests
make test

# Run tests with concise output (for AI agents)
make test-concise
```

### Building

```bash
# Build package
make build
```

## Pre-commit Checks

Before committing, run:

```bash
make precommit
```

This runs linting and type checking.

## Project Layout

```text
src/
└── key_value/
    └── aio/           # Async key-value library
tests/                 # Test suite
scripts/               # Development scripts
```

## Releasing

TBD
