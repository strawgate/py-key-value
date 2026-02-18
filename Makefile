.PHONY: bump-version bump-version-dry lint typecheck sync precommit test build help
.PHONY: install test-concise docs-serve docs-build docs-deploy setup

# Use python -m uv for portability (works even when uv isn't in PATH)
UV = python -m uv

# Default target - show help
.DEFAULT_GOAL := help

# Help target
help:
	@echo "Available targets:"
	@echo "  make help              - Show this help message"
	@echo "  make sync              - Install all dependencies"
	@echo "  make install           - Alias for sync"
	@echo "  make setup             - Setup environment (installs uv if needed)"
	@echo "  make lint              - Run linters (Python + Markdown)"
	@echo "  make typecheck         - Run type checking"
	@echo "  make test              - Run all tests (verbose)"
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
	@$(UV) run python scripts/bump_versions.py $(VERSION)

bump-version-dry:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, usage: make bump-version-dry VERSION=1.2.3"; exit 1; fi
	@echo "Bumping version (dry run)..."
	@$(UV) run python scripts/bump_versions.py $(VERSION) --dry-run

# Lint target
lint:
	@echo "Linting..."
	@$(UV) run ruff format
	@$(UV) run ruff check --fix
	@markdownlint --fix -c .markdownlint.jsonc .

# Type check target
typecheck:
	@echo "Type checking..."
	@$(UV) run basedpyright

# Sync target
sync:
	@echo "Syncing dependencies..."
	@$(UV) sync --group dev
	@npm install -g markdownlint-cli

# Install is an alias for sync
install: sync

# Setup environment (installs uv if needed, then syncs)
setup:
	@$(UV) --version >/dev/null 2>&1 || pip install uv
	@$(UV) sync --group dev

# Test target
test:
	@echo "Running tests..."
	@$(UV) run pytest tests -vv

# Concise test output for AI agents
test-concise:
	@echo "Running tests (concise output)..."
	@$(UV) run pytest tests -qq --tb=line --no-header

# Build target
build:
	@echo "Building package..."
	@$(UV) build

precommit: lint typecheck

# Documentation targets
docs-serve:
	@echo "Starting documentation server..."
	@$(UV) run --extra docs mkdocs serve

docs-build:
	@echo "Building documentation..."
	@$(UV) run --extra docs mkdocs build

docs-deploy:
	@echo "Deploying documentation to GitHub Pages..."
	@$(UV) run --extra docs mkdocs gh-deploy --force
