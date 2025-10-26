.PHONY: bump-version bump-version-dry codegen lint typecheck sync setup install test test-aio test-sync precommit


bump-version:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, usage: make bump-version VERSION=1.2.3"; exit 1; fi
	@echo "Bumping version..."
	@uv run python scripts/bump_versions.py $(VERSION)

bump-version-dry:
	@if [ -z "$(VERSION)" ]; then echo "VERSION is required, usage: make bump-version-dry VERSION=1.2.3"; exit 1; fi
	@echo "Bumping version (dry run)..."
	@uv run python scripts/bump_versions.py $(VERSION) --dry-run

codegen:
	@echo "Codegen..."
	@uv run python scripts/build_sync_library.py

lint:
	@echo "Linting..."
	@uv run ruff format
	@uv run ruff check --fix
	@markdownlint  --fix -c .markdownlint.jsonc .

typecheck:
	@echo "Type checking..."
	@uv run basedpyright

sync:
	@echo "Syncing..."
	@uv sync --all-packages
	@npm install -g markdownlint-cli

# Alias for sync (for consistency with common conventions)
setup: sync

install: sync

test:
	@echo "Running all tests..."
	@uv run pytest key-value/key-value-aio/tests -vv
	@uv run pytest key-value/key-value-sync/tests -vv

test-aio:
	@echo "Running async tests..."
	@uv run pytest key-value/key-value-aio/tests -vv

test-sync:
	@echo "Running sync tests..."
	@uv run pytest key-value/key-value-sync/tests -vv

precommit: lint typecheck codegen