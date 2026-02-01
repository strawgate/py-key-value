# Code Style

This document describes code style conventions that are **not enforced by
automated tooling**. For tool-enforced rules, just run `make lint` and
`make typecheck`.

## Make Commands

| Command | What it does |
| ------- | ------------ |
| `make lint` | Ruff formatting/linting (Python) + markdownlint (Markdown) |
| `make typecheck` | Basedpyright strict type checking |
| `make precommit` | Both of the above |

## Python Conventions

### ManagedEntry Wrapper

Raw values are **never** stored directly in backends. All values are wrapped in
`ManagedEntry` objects (from `key_value.shared.managed_entry`) which include
metadata like creation timestamp and TTL. This is intentional - don't try to
"fix" it.

### Async Patterns

- **Production stores**: Use actual async I/O. Avoid `asyncio.sleep()` for
  artificial delays or `run_in_executor()` when async alternatives exist.
- **Debug/development tools**: Simpler patterns are acceptable for clarity.

### Connection Ownership

When stores accept external client connections, track ownership to avoid closing
externally-provided resources. Use a flag like `_owns_client` to distinguish
between internally-created and externally-provided connections.

### Clock Usage

- **Intervals/timeouts**: Use `time.monotonic()` - immune to system clock
  changes
- **Timestamps for storage**: Use `time.time()` when you need wall-clock time

### Test Patterns

- Use `ContextManagerStoreTestMixin` for store tests to ensure consistency
- Ensure tests clean up resources and don't interfere with each other
- Use context managers and proper teardown

## Markdown Conventions

Line length is 80 characters, excluding code blocks and tables. The linter
handles this, but keep it in mind when writing.
