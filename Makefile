.PHONY: help
help:
	@echo "Make targets for Redis Streams Cleaner"
	@echo "make init - Set up dev environment"
	@echo "make update - Update pinned dependencies and run make init"
	@echo "make update-deps - Update pinned dependencies"

.PHONY: init
init:
	uv sync --frozen --all-groups
	uv run prek install

.PHONY: update
update: update-deps init
