"""Snowflake connection (key-pair auth) and a connectivity check."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.config import ConfigError, SnowflakeConfig

logger = logging.getLogger(__name__)

QUERY_TAG = "nyc_taxi_elt"


@contextmanager
def connect(config: SnowflakeConfig) -> Iterator[SnowflakeConnection]:
    """Open a connection as the pipeline's service user and always close it."""
    if not config.private_key_path.is_file():
        raise ConfigError(
            f"Private key not found at {config.private_key_path}. "
            "Generate it into ./secrets (see README, 'Snowflake setup')."
        )
    connection = snowflake.connector.connect(
        account=config.account,
        user=config.user,
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(config.private_key_path),
        private_key_file_pwd=config.private_key_passphrase,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.raw_schema,
        session_parameters={"QUERY_TAG": QUERY_TAG, "TIMEZONE": "UTC"},
    )
    try:
        yield connection
    finally:
        connection.close()


def check_connection(config: SnowflakeConfig) -> dict[str, str]:
    """Connect and report the session context. Uses no warehouse compute."""
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
            "CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()"
        )
        row = cursor.fetchone()
    keys = ("user", "role", "warehouse", "database", "schema", "snowflake_version")
    context = dict(zip(keys, (str(v) for v in row), strict=True))
    logger.info("Snowflake connection OK", extra=context)
    return context
