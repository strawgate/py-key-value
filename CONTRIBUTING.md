# Contributing

Thank you for contributing to py-key-value!

## Getting Started

See [DEVELOPING.md](DEVELOPING.md) for development setup.

## Code Style

See [CODE_STYLE.md](CODE_STYLE.md) for conventions not enforced by tooling.

For tool-enforced rules, just run:

```bash
make precommit
```

## Pull Request Process

1. **File an issue first** for non-trivial changes
2. **Run checks before submitting**:

   ```bash
   make precommit  # lint + typecheck
   make test       # run tests
   ```

3. **Self-review your changes** before requesting review

## Issue Reporting

Use GitHub Issues for bug reports and feature requests. Include:

- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Python version and relevant dependencies

## Working with Code Review Feedback

This project uses AI-assisted code review (CodeRabbit) and development (Claude).
This section provides guidance on handling automated code review feedback.

### Triage Before Acting

Always categorize feedback before implementation:

- **Critical**: Security issues, data corruption, resource leaks, production bugs
- **Important**: Type safety, error handling, performance issues, test failures
- **Optional**: Style preferences, nitpicks, suggestions that conflict with
  existing patterns

### Evaluate Against Existing Patterns

Before accepting suggestions:

1. Search the codebase for similar patterns
2. Check if other stores/wrappers handle this differently
3. Preserve consistency over isolated "best practices"
4. If uncertain, ask the repository owner

**Example**: Test patterns should match existing `ContextManagerStoreTestMixin`
usage rather than one-off suggestions for individual test files.

### Consider Context and Scope

Not all code has the same requirements:

- **Production stores**: Prioritize correctness, performance, security
- **Debug/development tools**: Can defer async optimization, extensive validation
- **Test code**: Clarity and coverage over production patterns
- **Examples**: Simplicity and readability over comprehensive error handling

### Verify Completion

Before claiming work is "ready to merge" or "complete":

- [ ] All critical issues addressed or documented as out-of-scope
- [ ] All important issues addressed or explicitly deferred with rationale
- [ ] No unrelated changes from bad merges
- [ ] `make precommit` passes (lint, typecheck)
- [ ] Tests pass

### Document Deferrals

If feedback is not implemented, explain why:

- Conflicts with established pattern (cite similar code)
- Out of scope for this component's purpose
- Trade-off not worth the complexity
- Requires design decision from repository owner

## For AI Code Reviewers (CodeRabbit)

When reviewing pull requests, please consider:

### Project-Specific Patterns

- **Async-only architecture**: All code is in `src/key_value/aio/`.
- **Test patterns**: The project uses `ContextManagerStoreTestMixin` for store
  tests. Look for consistency with existing test implementations.
- **ManagedEntry wrapper**: Values are never stored directly but are wrapped in
  `ManagedEntry` objects. This is by design, not a mistake.

### Prioritization Guidance

When providing feedback, please categorize suggestions by severity:

- **Critical**: Issues that could cause data loss, security vulnerabilities,
  resource leaks, or production failures
- **Important**: Type safety issues, missing error handling, performance
  problems, test coverage gaps
- **Minor/Optional**: Style preferences, minor optimizations, suggestions that
  may conflict with existing patterns

This helps human developers and AI agents prioritize their work effectively.

### Context Awareness

Consider the purpose and scope of the code being reviewed:

- **Production stores** (DynamoDB, Redis, PostgreSQL, etc.): Apply strict
  standards for correctness, performance, security, and resource management
- **Debug/development tools** (FileTreeStore, LoggingWrapper): More lenient on
  performance optimizations; prioritize clarity and simplicity
- **Test code**: Focus on clarity, coverage, and maintainability over production
  patterns
- **Example code**: Prioritize readability and educational value over
  comprehensive error handling

### Pattern Consistency

Before suggesting changes:

1. Check if similar patterns exist elsewhere in the codebase
2. If the pattern exists in multiple stores/wrappers, it's likely intentional
3. Suggest consistency improvements across all implementations rather than
   one-off changes
