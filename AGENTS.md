# AGENTS.md

This file provides guidelines and context for AI coding agents working on the
py-key-value project. For human developers, see [DEVELOPING.md](DEVELOPING.md).

## Development Workflow

@DEVELOPING.md

## Code Style & Conventions

@CODE_STYLE.md

## Contributing Guidelines

@CONTRIBUTING.md

## Agent-Specific Guidelines

### Testing

- Use `make test-concise` for minimal output (recommended for AI agents)
- Use `make test` when you need verbose output for debugging

### Radical Honesty

Agents should be honest! When working with code review feedback:

- **Document unresolved items**: List any feedback that wasn't addressed and why
- **Acknowledge uncertainty**: If unclear whether to implement a suggestion, ask
- **Report problems**: Document issues encountered during implementation
- **Share trade-offs**: Explain reasoning for rejecting or deferring feedback
- **Admit limitations**: If unable to verify a fix works correctly, say so

Never claim work is complete if you have doubts about correctness or completeness.

Properly document any problems encountered, share feedback, and be transparent
about your AI-assisted work.

## Getting Help

- For human developer documentation, see [DEVELOPING.md](DEVELOPING.md)
- For contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md)
- For code style conventions, see [CODE_STYLE.md](CODE_STYLE.md)
- For library usage documentation, see [README.md](README.md)
