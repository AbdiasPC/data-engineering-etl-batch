.DEFAULT_GOAL := help
.PHONY: help build up down shell lint test clean check-connection extract load transform \
	validate pipeline analytics docs docs-serve dbt

# Optional month range override, e.g. `make extract START=2025-01 END=2025-01`.
# FORCE=1 reloads files even when an identical copy is already in RAW.
RANGE := $(if $(START),--start $(START)) $(if $(END),--end $(END)) $(if $(FORCE),--force)

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

load: .env ## Stage files and COPY into RAW (idempotent, reconciled)
	$(RUN) python -m src.pipeline load $(RANGE)

transform: .env ## dbt deps, source freshness, build (models + tests)
	$(RUN) python -m src.pipeline transform

validate: .env ## Run sql/validation checks; non-zero exit if any fails
	$(RUN) python -m src.pipeline validate

pipeline: .env ## Run everything: extract > load > transform > validate
	$(RUN) python -m src.pipeline all $(RANGE)

analytics: .env ## Print the answers to the business questions in sql/analytics
	$(RUN) python -m src.pipeline analytics

docs: .env ## Generate dbt docs and check every model and column is described
	$(RUN) sh -c "dbt deps && dbt docs generate && python scripts/check_dbt_docs.py"

docs-serve: .env ## Serve dbt docs at http://localhost:8080
	$(COMPOSE) run --rm -p 8080:8080 pipeline dbt docs serve --host 0.0.0.0 --port 8080

dbt: .env ## Run any dbt command, e.g. make dbt ARGS="build --full-refresh"
	$(RUN) dbt $(ARGS)

clean: ## Remove downloaded data and build artifacts (local files only)
	rm -rf data/* dbt/target dbt/dbt_packages dbt/logs logs .pytest_cache .ruff_cache
