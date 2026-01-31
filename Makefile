.PHONY: bump-version bump-version-dry lint typecheck sync precommit test build help
.PHONY: install test-aio test-shared test-concise test-aio-concise test-shared-concise docs-serve docs-build docs-deploy

# Default target - show help
.DEFAULT_GOAL := help

# Help target
help:
	@echo "Available targets:"
	@echo "  make help              - Show this help message"
	@echo "  make sync              - Install all dependencies (all packages)"
	@echo "  make install           - Alias for sync"
	@echo "  make lint              - Run linters (Python + Markdown)"
	@echo "  make typecheck         - Run type checking"
	@echo "  make test              - Run all tests (verbose)"
	@echo "  make test-concise      - Run all tests (concise output for AI agents)"
	@echo "  make test-aio          - Run async package tests"
	@echo "  make test-aio-concise  - Run async package tests (concise)"
	@echo "  make test-shared       - Run shared package tests"
	@echo "  make test-shared-concise - Run shared package tests (concise)"
	@echo "  make build             - Build all packages"
	@echo "  make precommit         - Run pre-commit checks (lint + typecheck)"
	@echo "  make docs-serve        - Start documentation server"
	@echo "  make docs-build        - Build documentation"
	@echo "  make docs-deploy       - Deploy documentation to GitHub Pages"
	@echo ""
	@echo "Per-project targets (use PROJECT=<path>):"
	@echo "  make sync PROJECT=key-value/key-value-aio"
	@echo "  make lint PROJECT=key-value/key-value-aio"
	@echo "  make typecheck PROJECT=key-value/key-value-aio"
	@echo "  make test PROJECT=key-value/key-value-aio"
	@echo "  make build PROJECT=key-value/key-value-aio"
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

# Lint target - supports PROJECT parameter
lint:
ifdef PROJECT
	@echo "Linting $(PROJECT)..."
	@cd $(PROJECT) && uv run ruff format .
	@cd $(PROJECT) && uv run ruff check --fix .
else
	@echo "Linting all projects..."
	@uv run ruff format
	@uv run ruff check --fix
	@markdownlint --fix -c .markdownlint.jsonc .
endif

# Type check target - supports PROJECT parameter
typecheck:
ifdef PROJECT
	@echo "Type checking $(PROJECT)..."
	@cd $(PROJECT) && uv run basedpyright .
else
	@echo "Type checking all projects..."
	@uv run basedpyright
endif

# Sync target - supports PROJECT parameter
sync:
ifdef PROJECT
	@echo "Syncing $(PROJECT)..."
	@cd $(PROJECT) && uv sync --locked --group dev
else
	@echo "Syncing all packages..."
	@uv sync --all-packages --group dev
	@npm install -g markdownlint-cli
endif

# Install is an alias for sync
install: sync

# Test target - supports PROJECT parameter
test:
ifdef PROJECT
	@echo "Testing $(PROJECT)..."
	@cd $(PROJECT) && uv run pytest tests . -vv
else
	@echo "Testing all packages..."
	@uv run pytest key-value/key-value-aio/tests -vv
	@uv run pytest key-value/key-value-shared/tests -vv
endif

# Convenience targets for specific packages
test-aio:
	@echo "Testing key-value-aio..."
	@uv run pytest key-value/key-value-aio/tests -vv

test-shared:
	@echo "Testing key-value-shared..."
	@uv run pytest key-value/key-value-shared/tests -vv

# Concise test output for AI agents - supports PROJECT parameter
test-concise:
ifdef PROJECT
	@echo "Testing $(PROJECT) (concise output)..."
	@cd $(PROJECT) && uv run pytest tests . -qq --tb=line --no-header
else
	@echo "Testing all packages (concise output)..."
	@uv run pytest key-value/key-value-aio/tests -qq --tb=line --no-header
	@uv run pytest key-value/key-value-shared/tests -qq --tb=line --no-header
endif

# Convenience targets for specific packages with concise output
test-aio-concise:
	@echo "Testing key-value-aio (concise output)..."
	@uv run pytest key-value/key-value-aio/tests -qq --tb=line --no-header

test-shared-concise:
	@echo "Testing key-value-shared (concise output)..."
	@uv run pytest key-value/key-value-shared/tests -qq --tb=line --no-header

# Build target - supports PROJECT parameter
build:
ifdef PROJECT
	@echo "Building $(PROJECT)..."
	@cd $(PROJECT) && uv build .
else
	@echo "Building all packages..."
	@cd key-value/key-value-aio && uv build .
	@cd key-value/key-value-shared && uv build .
endif

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
