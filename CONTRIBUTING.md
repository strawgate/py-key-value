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
