.PHONY: bump-version bump-version-dry lint typecheck sync precommit test build help
.PHONY: install test-concise test-unit test-integration docs-serve docs-build docs-deploy

# Default target - show help
.DEFAULT_GOAL := help

# Help target
help:
	@echo "Available targets:"
	@echo "  make help              - Show this help message"
	@echo "  make sync              - Install all dependencies"
	@echo "  make install           - Alias for sync"
	@echo "  make lint              - Run linters (Python + Markdown)"
	@echo "  make typecheck         - Run type checking"
	@echo "  make test              - Run all tests (verbose)"
	@echo "  make test-unit         - Run unit tests only (no Docker)"
	@echo "  make test-integration  - Run integration tests only (requires Docker)"
	@echo "  make test-concise      - Run all tests (concise output for AI agents)"
	@echo "  make build             - Build package"
	@echo "  make precommit         - Run pre-commit checks (lint + typecheck)"
	@echo "  make docs-serve        - Start documentation server"
	@echo "  make docs-build        - Build documentation"
	@echo "  make docs-deploy       - Deploy documentation to GitHub Pages"
	@echo ""
	@echo "Version management:"
	@echo "  make bump-version VERSION=1.2.3     - Bump version"
	@echo "  make bump-version-dry VERSION=1.2.3 - Dry run version bump"

bump-version:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, usage: make bump-version VERSION=1.2.3"; exit 1; fi
	@echo "Bumping version..."
	@uv run python scripts/bump_versions.py $(VERSION)

bump-version-dry:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, usage: make bump-version-dry VERSION=1.2.3"; exit 1; fi
	@echo "Bumping version (dry run)..."
	@uv run python scripts/bump_versions.py $(VERSION) --dry-run

# Lint target
lint:
	@echo "Linting..."
	@uv run ruff format
	@uv run ruff check --fix
	@markdownlint --fix -c .markdownlint.jsonc .

# Type check target
typecheck:
	@echo "Type checking..."
	@uv run basedpyright

# Sync target
sync:
	@echo "Syncing dependencies..."
	@uv sync --group dev
	@npm install -g markdownlint-cli

# Install is an alias for sync
install: sync

# Test target
test:
	@echo "Running tests..."
	@uv run pytest tests -vv

# Unit tests only (no Docker required)
test-unit:
	@echo "Running unit tests..."
	@uv run pytest tests -vv -m "not integration"

# Integration tests only (requires Docker)
test-integration:
	@echo "Running integration tests..."
	@uv run pytest tests -vv -m "integration"

# Concise test output for AI agents
test-concise:
	@echo "Running tests (concise output)..."
	@uv run pytest tests -qq --tb=line --no-header

# Build target
build:
	@echo "Building package..."
	@uv build

precommit: lint typecheck

# Documentation targets
docs-serve:
	@echo "Starting documentation server..."
	@uv run --extra docs mkdocs serve

docs-build:
	@echo "Building documentation..."
	@uv run --extra docs mkdocs build

docs-deploy:
	@echo "Deploying documentation to GitHub Pages..."
	@uv run --extra docs mkdocs gh-deploy --force
