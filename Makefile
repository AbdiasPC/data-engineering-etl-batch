.DEFAULT_GOAL := help
.PHONY: help build up down shell lint test clean check-connection extract

# Optional month range override, e.g. `make extract START=2025-01 END=2025-01`.
RANGE := $(if $(START),--start $(START)) $(if $(END),--end $(END))

COMPOSE := docker compose
RUN := $(COMPOSE) run --rm pipeline

help: ## Show available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

.env:
	@echo "Missing .env. Run: cp .env.example .env  (then fill in the Snowflake values)"; exit 1

build: .env ## Build the Docker image
	$(COMPOSE) build

up: .env ## Start an idle dev container (use `make shell`)
	$(COMPOSE) up -d

down: ## Stop and remove containers
	$(COMPOSE) down --remove-orphans

shell: .env ## Open a shell in a one-off container
	$(RUN) bash

lint: .env ## Ruff lint and format check
	$(RUN) sh -c "ruff check . && ruff format --check ."

test: .env ## Lint plus unit tests (no network, no Snowflake)
	$(RUN) sh -c "ruff check . && ruff format --check . && pytest"

check-connection: .env ## Verify Snowflake connectivity as the pipeline role
	$(RUN) python -m src.pipeline check-connection

extract: .env ## Download and validate source files into ./data
	$(RUN) python -m src.pipeline extract $(RANGE)

clean: ## Remove downloaded data and build artifacts (local files only)
	rm -rf data/* dbt/target dbt/dbt_packages dbt/logs logs .pytest_cache .ruff_cache
