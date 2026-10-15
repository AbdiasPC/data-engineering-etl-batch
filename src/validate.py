"""Run the SQL files under sql/validation (pass/fail checks) and sql/analytics (reports).

SQL files refer to schemas as {raw}, {staging} and {marts}; these are replaced with the
configured schema names, which config.py has already validated as plain identifiers.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from snowflake.connector import SnowflakeConnection

from src.config import SnowflakeConfig
from src.snowflake_client import connect

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
VALIDATION_DIR = SQL_DIR / "validation"
ANALYTICS_DIR = SQL_DIR / "analytics"
CHECK_COLUMNS = ["CHECK_NAME", "PASSED", "DETAILS"]


class ValidationError(RuntimeError):
    """One or more validation checks failed."""


def render_sql(path: Path, config: SnowflakeConfig) -> str:
    return path.read_text(encoding="utf-8").format(
        raw=config.raw_schema, staging=config.staging_schema, marts=config.marts_schema
    )


def run_query(connection: SnowflakeConnection, sql: str) -> tuple[list[str], list[tuple]]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return [column.name for column in cursor.description], cursor.fetchall()


def format_table(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    """Render a result set as a plain-text table."""
    cells = [[("" if value is None else str(value)) for value in row] for row in rows]
    widths = [max([len(c), *(len(r[i]) for r in cells)]) for i, c in enumerate(columns)]
    lines = [
        "  ".join(name.ljust(width) for name, width in zip(columns, widths, strict=True)),
        "  ".join("-" * width for width in widths),
    ]
    lines += ["  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)) for row in cells]
    return "\n".join(lines)


def run_validation(config: SnowflakeConfig) -> None:
    """Run every validation file. Each returns rows of (check_name, passed, details)."""
    failures: list[str] = []
    total = 0
    with connect(config) as connection:
        for path in sorted(VALIDATION_DIR.glob("*.sql")):
            columns, rows = run_query(connection, render_sql(path, config))
            if columns != CHECK_COLUMNS:
                raise ValidationError(f"{path.name} must return {CHECK_COLUMNS}, got {columns}")
            for name, passed, details in rows:
                total += 1
                logger.log(
                    logging.INFO if passed else logging.ERROR,
                    "Validation %s: %s",
                    "PASS" if passed else "FAIL",
                    name,
                    extra={"check": name, "passed": bool(passed), "details": details},
                )
                if not passed:
                    failures.append(f"{name} ({details})")
    if failures:
        raise ValidationError(f"{len(failures)} of {total} checks failed: {'; '.join(failures)}")
    logger.info("All validation checks passed", extra={"checks": total})


def run_analytics(config: SnowflakeConfig) -> None:
    """Run every analytics query and write its result to stdout as a text table."""
    with connect(config) as connection:
        for path in sorted(ANALYTICS_DIR.glob("*.sql")):
            sql = render_sql(path, config)
            title = sql.splitlines()[0].lstrip("- ").strip()
            columns, rows = run_query(connection, sql)
            sys.stdout.write(f"\n== {path.name}: {title}\n{format_table(columns, rows)}\n")
