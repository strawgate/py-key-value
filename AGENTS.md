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
- Run `make test` to execute all test suites (verbose output)
- Run `make test-concise` for minimal output (recommended for AI agents)
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

## Working with Code Review Feedback

This project uses AI-assisted code review (CodeRabbit) and development (Claude).
This section provides guidance for both AI agents and human developers on how
to handle automated code review feedback effectively.

### For AI Coding Agents (Claude)

When responding to CodeRabbit feedback on pull requests:

#### 1. Triage Before Acting

Always categorize feedback before implementation:

- **Critical**: Security issues, data corruption, resource leaks, production bugs
- **Important**: Type safety, error handling, performance issues, test failures
- **Optional**: Style preferences, nitpicks, suggestions that conflict with
  existing patterns

Document your triage in the response to the user.

#### 2. Evaluate Against Existing Patterns

Before accepting suggestions:

1. Search the codebase for similar patterns
2. Check if other stores/wrappers handle this differently
3. Preserve consistency over isolated "best practices"
4. If uncertain, ask the repository owner

**Example**: Test patterns should match existing `ContextManagerStoreTestMixin`
usage rather than one-off suggestions for individual test files.

#### 3. Consider Context and Scope

Not all code has the same requirements:

- **Production stores**: Prioritize correctness, performance, security
- **Debug/development tools**: Can defer async optimization, extensive validation
- **Test code**: Clarity and coverage over production patterns
- **Examples**: Simplicity and readability over comprehensive error handling

#### 4. Verify Completion

Before claiming work is "ready to merge" or "complete":

- [ ] All critical issues addressed or documented as out-of-scope
- [ ] All important issues addressed or explicitly deferred with rationale
- [ ] No unrelated changes from bad merges
- [ ] `make precommit` passes (lint, typecheck, codegen)
- [ ] Tests pass

Never claim completion with unresolved critical or important issues.

#### 5. Document Deferrals

If feedback is not implemented, explain why:

- Conflicts with established pattern (cite similar code)
- Out of scope for this component's purpose
- Trade-off not worth the complexity
- Requires design decision from repository owner

### For AI Code Reviewers (CodeRabbit)

When reviewing pull requests, please consider:

#### Project-Specific Patterns

- **Async-first architecture**: All changes originate in `key-value/key-value-aio/`
  and are code-generated into `key-value/key-value-sync/`. PRs will naturally
  contain duplicate changes - this is intentional.
- **Test patterns**: The project uses `ContextManagerStoreTestMixin` for store
  tests. Look for consistency with existing test implementations.
- **ManagedEntry wrapper**: Values are never stored directly but are wrapped in
  `ManagedEntry` objects. This is by design, not a mistake.

#### Prioritization Guidance

When providing feedback, please categorize suggestions by severity:

- **Critical**: Issues that could cause data loss, security vulnerabilities,
  resource leaks, or production failures
- **Important**: Type safety issues, missing error handling, performance problems,
  test coverage gaps
- **Minor/Optional**: Style preferences, minor optimizations, suggestions that
  may conflict with existing patterns

This helps human developers and AI agents prioritize their work effectively.

#### Context Awareness

Consider the purpose and scope of the code being reviewed:

- **Production stores** (DynamoDB, Redis, PostgreSQL, etc.): Apply strict
  standards for correctness, performance, security, and resource management
- **Debug/development tools** (FileTreeStore, LoggingWrapper): More lenient on
  performance optimizations; prioritize clarity and simplicity
- **Test code**: Focus on clarity, coverage, and maintainability over production
  patterns
- **Example code**: Prioritize readability and educational value over
  comprehensive error handling

#### Pattern Consistency

Before suggesting changes:

1. Check if similar patterns exist elsewhere in the codebase
2. If the pattern exists in multiple stores/wrappers, it's likely intentional
3. Suggest consistency improvements across all implementations rather than
   one-off changes

### Common Feedback Categories

**Clock usage**: Prefer monotonic clocks (`time.monotonic()`) for intervals,
especially in wrappers like rate limiters and circuit breakers. Wall-clock time
(`time.time()`) is vulnerable to system clock changes.

**Connection ownership**: Track whether stores own their client connections to
avoid closing externally-provided resources. Use flags like `_owns_client` to
distinguish between internally-created and externally-provided connections.

**Async patterns**: Production stores should use actual async I/O (not
`asyncio.sleep()` or `run_in_executor()`). Debug-only tools may use simpler
patterns for clarity.

**Test isolation**: Ensure tests clean up resources (connections, temp files,
etc.) and don't interfere with each other. Use context managers and proper
teardown.

**Type safety**: This project uses strict type checking (Basedpyright). Address
type annotation issues to maintain type safety guarantees.

## Make Commands Reference

| Command | Purpose |
| --------- | --------- |
| `make sync` | Install all dependencies |
| `make install` | Alias for `make sync` |
| `make lint` | Lint Python + Markdown |
| `make typecheck` | Run Basedpyright type checking |
| `make test` | Run all test suites (verbose) |
| `make test-concise` | Run all test suites (concise output for AI) |
| `make test-aio` | Run async package tests |
| `make test-aio-concise` | Run async package tests (concise) |
| `make test-sync` | Run sync package tests |
| `make test-sync-concise` | Run sync package tests (concise) |
| `make test-shared` | Run shared package tests |
| `make test-shared-concise` | Run shared package tests (concise) |
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
- `claude-on-test-failure.yml` - Claude test failure analysis (automatically
  analyzes failed tests and suggests solutions)

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

Agents should be honest! When working with code review feedback:

- **Document unresolved items**: List any feedback that wasn't addressed and why
- **Acknowledge uncertainty**: If unclear whether to implement a suggestion, ask
- **Report problems**: Document issues encountered during implementation
- **Share trade-offs**: Explain reasoning for rejecting or deferring feedback
- **Admit limitations**: If unable to verify a fix works correctly, say so

Never claim work is complete if you have doubts about correctness or completeness.

Properly document any problems encountered, share feedback, and be transparent
about your AI-assisted work.
