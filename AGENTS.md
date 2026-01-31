# AGENTS.md

This file provides guidelines and context for AI coding agents working on the
py-key-value project. For human developers, see [DEVELOPING.md](DEVELOPING.md).

## Development Workflow

### Required Pre-commit Checks

All checks must pass before committing:

1. `make lint` - Runs Ruff formatting and linting (Python + Markdown)
2. `make typecheck` - Runs Basedpyright type checking

Or run both together:

```bash
make precommit
```

### Testing Requirements

- All new features require tests
- Run `make test` to execute all test suites (verbose output)
- Run `make test-concise` for minimal output (recommended for AI agents)
- Test coverage should be maintained

## Architecture

### Project Structure

```text
src/
└── key_value/
    └── aio/               # Async key-value library
        ├── adapters/      # Type adapters (Pydantic, Dataclass, etc.)
        ├── errors/        # Error classes
        ├── protocols/     # Protocol definitions
        ├── stores/        # Backend implementations
        ├── utils/         # Utility modules
        └── wrappers/      # Store wrappers
tests/                     # Test suite
scripts/
└── bump_versions.py       # Version management script
```

## Code Style & Conventions

### Python

- **Formatter/Linter**: Ruff (configured in `pyproject.toml`)
- **Line length**: 140 characters
- **Type checker**: Basedpyright (strict mode)
- **Runtime type checking**: Beartype (can be disabled via
  `PY_KEY_VALUE_DISABLE_BEARTYPE=true`)
- **Python version**: 3.10+

### Markdown

- **Linter**: markdownlint (`.markdownlint.jsonc`)
- **Line length**: 80 characters (excluding code blocks and tables)

## Common Pitfalls

### ManagedEntry Wrapper Objects

Raw values are **NEVER** stored directly in backends. The `ManagedEntry` wrapper
(from `key_value.aio.utils.managed_entry`) wraps values with metadata
like TTL and creation timestamp, typically serialized to/from JSON.

When implementing or debugging stores, remember that what's stored is not
the raw value but a `ManagedEntry` containing:

- The actual value
- Creation timestamp
- TTL metadata

### Optional Backend Dependencies

Store implementations have optional dependencies. Install extras as needed:

```bash
pip install py-key-value-aio[redis]      # Redis support
pip install py-key-value-aio[dynamodb]   # DynamoDB support
pip install py-key-value-aio[mongodb]    # MongoDB support
# etc. - see README.md for full list
```

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
- [ ] `make precommit` passes (lint, typecheck)
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

- **Async-only architecture**: All code is in `src/key_value/aio/`.
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
| ------- | ------- |
| `make sync` | Install all dependencies |
| `make install` | Alias for `make sync` |
| `make lint` | Lint Python + Markdown |
| `make typecheck` | Run Basedpyright type checking |
| `make test` | Run all tests (verbose) |
| `make test-concise` | Run all tests (concise output for AI) |
| `make precommit` | Run lint + typecheck |
| `make build` | Build package |

## Key Protocols and Interfaces

### AsyncKeyValue Protocol

The core interface is `AsyncKeyValue` protocol from
`key_value.aio.protocols.key_value`. All stores implement this
protocol, which defines:

- `get`, `get_many` - Retrieve values
- `put`, `put_many` - Store values with optional TTL
- `delete`, `delete_many` - Remove values
- `ttl`, `ttl_many` - Get TTL information

## Store Implementations

Stores are located in `src/key_value/aio/stores/`.

Available backends include: DynamoDB, Elasticsearch, Memcached, Memory, Disk,
MongoDB, Redis, RocksDB, Valkey, Vault, Windows Registry, Keyring, and more.

## Wrappers

Wrappers add functionality to stores and are located in
`src/key_value/aio/wrappers/`.

Wrappers include: Compression, DefaultValue, Encryption, Logging, Statistics,
Retry, Timeout, Cache, Prefix, TTL clamping, and more.

## Adapters

Adapters simplify store interactions but don't implement the protocol directly.
Located in `src/key_value/aio/adapters/`.

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

## Getting Help

- For human developer documentation, see [DEVELOPING.md](DEVELOPING.md)
- For library usage documentation, see [README.md](README.md)

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
