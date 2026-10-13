from pathlib import Path

import pytest

from src import validate
from src.config import SnowflakeConfig

CONFIG = SnowflakeConfig(
    account="a",
    user="u",
    private_key_path=Path("/k"),
    role="R",
    warehouse="W",
    database="D",
    raw_schema="RAW_X",
    staging_schema="STG_X",
    marts_schema="MARTS_X",
)


@pytest.mark.parametrize(
    "path", sorted([*validate.VALIDATION_DIR.glob("*.sql"), *validate.ANALYTICS_DIR.glob("*.sql")])
)
def test_every_sql_file_renders_with_configured_schemas(path):
    sql = validate.render_sql(path, CONFIG)
    assert "{" not in sql
    assert "MARTS_X." in sql or "RAW_X." in sql
    assert sql.startswith("-- ")  # first line is the title shown in reports


def test_sql_directories_are_not_empty():
    assert len(list(validate.VALIDATION_DIR.glob("*.sql"))) >= 3
    assert len(list(validate.ANALYTICS_DIR.glob("*.sql"))) >= 4


def test_format_table_aligns_columns_and_blanks_nulls():
    table = validate.format_table(["NAME", "N"], [("a", 1), ("longer", None)])
    assert table.splitlines() == ["NAME    N", "------  -", "a       1", "longer   "]
