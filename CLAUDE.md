# CLAUDE.md

Batch ELT portfolio project: Python extracts NYC TLC yellow taxi trips, the taxi zone lookup, and Open-Meteo daily weather; loads them unmodified into Snowflake `RAW`; dbt builds `STAGING` and `MARTS`. Everything runs in Docker via `make`.

## Commands

All commands run inside the container; do not install anything on the host.

| Command | Purpose |
|---|---|
| `make build` | Build the image (needs `.env`; `cp .env.example .env`) |
| `make test` | `ruff check`, `ruff format --check`, `pytest` |
| `make shell` | Shell in a one-off container |
| `make help` | List every target |

Month range override on any pipeline target: `make extract START=2025-01 END=2025-01`.

## Conventions

- **Config and secrets**: environment variables only, read in `src/config.py`. Never hardcode, commit, or log credentials. Snowflake auth is key-pair; the key lives in `secrets/` (gitignored) and is mounted read-only at `/run/secrets`.
- **Snowflake identifiers** come from env vars and are validated in `config.py` before being interpolated into SQL. Values are always bound parameters.
- **RAW is as-is**: no cleaning or filtering in Python. Every RAW table carries `_SOURCE_FILE`, `_FILE_ROW_NUMBER`, `_BATCH_ID`, `_LOADED_AT`.
- **Loads are idempotent per file**: delete by `_SOURCE_FILE`, then `COPY`, then reconcile row counts, in one transaction.
- **dbt**: staging flags invalid records, it never drops them. Structural tests use severity `error`; known dirty-data rules use `warn`. Every model and column is documented in `schema.yml`.
- **Python**: type hints, small functions, JSON logs via `logging` with `extra=` fields, no `print`.
- **Tests**: no network and no Snowflake in unit tests; HTTP is mocked with `responses`.
- **Dependencies**: top-level pins in `requirements*.txt`, full set in `requirements.lock` (regenerate with `pip freeze` inside the image after changing a pin).
- **Git**: conventional commits, at least one per phase.
- **Admin DDL**: `sql/00_snowflake_setup.sql` is run manually by the account owner. The pipeline never runs as ACCOUNTADMIN and never touches objects outside the project database, warehouse, and role.

## Out of scope

Airflow/Dagster, streaming, external cloud stages, Terraform, BI dashboards.
