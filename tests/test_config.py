from datetime import date
from pathlib import Path

import pytest

from src.config import ConfigError, Month, PipelineConfig, SnowflakeConfig, month_range

BASE_ENV = {"PIPELINE_START_MONTH": "2025-01", "PIPELINE_END_MONTH": "2025-03"}

SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": "org-acct",
    "SNOWFLAKE_USER": "svc",
    "SNOWFLAKE_PRIVATE_KEY_PATH": "/run/secrets/rsa_key.p8",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "hunter2",
    "SNOWFLAKE_ROLE": "nyc_taxi_pipeline",
    "SNOWFLAKE_WAREHOUSE": "NYC_TAXI_WH",
    "SNOWFLAKE_DATABASE": "NYC_TAXI",
}


def test_month_parse_and_format():
    assert Month.parse("2025-01") == Month(2025, 1)
    assert str(Month(2025, 3)) == "2025-03"


@pytest.mark.parametrize("value", ["2025-1", "2025-13", "2025-00", "25-01", "2025/01", ""])
def test_month_parse_rejects_bad_input(value):
    with pytest.raises(ConfigError):
        Month.parse(value)


def test_month_boundaries_handle_leap_years():
    assert Month(2024, 2).last_day == date(2024, 2, 29)
    assert Month(2025, 2).last_day == date(2025, 2, 28)
    assert Month(2025, 2).first_day == date(2025, 2, 1)


def test_month_range_is_inclusive_and_crosses_years():
    months = month_range(Month(2024, 11), Month(2025, 2))
    assert [str(m) for m in months] == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_month_range_single_month():
    assert month_range(Month(2025, 1), Month(2025, 1)) == [Month(2025, 1)]


def test_month_range_rejects_reversed_and_oversized_ranges():
    with pytest.raises(ConfigError, match="before start"):
        month_range(Month(2025, 3), Month(2025, 1))
    with pytest.raises(ConfigError, match="limit"):
        month_range(Month(2020, 1), Month(2025, 1))


def test_pipeline_config_defaults_from_env():
    config = PipelineConfig.from_env(BASE_ENV)
    assert [str(m) for m in config.months] == ["2025-01", "2025-02", "2025-03"]
    assert config.data_dir == Path("data")
    assert (config.weather_latitude, config.weather_longitude) == (40.7128, -74.0060)


def test_cli_arguments_override_env():
    config = PipelineConfig.from_env(BASE_ENV, start="2025-02", end="2025-02")
    assert config.months == [Month(2025, 2)]


def test_pipeline_config_requires_month_range():
    with pytest.raises(ConfigError, match="PIPELINE_START_MONTH"):
        PipelineConfig.from_env({})


def test_snowflake_config_normalises_identifiers():
    config = SnowflakeConfig.from_env(SNOWFLAKE_ENV)
    assert config.role == "NYC_TAXI_PIPELINE"
    assert config.raw_schema == "RAW"


def test_snowflake_config_hides_passphrase_in_repr():
    assert "hunter2" not in repr(SnowflakeConfig.from_env(SNOWFLAKE_ENV))


def test_snowflake_config_rejects_unsafe_identifier():
    env = {**SNOWFLAKE_ENV, "SNOWFLAKE_DATABASE": "NYC; DROP DATABASE X"}
    with pytest.raises(ConfigError, match="identifier"):
        SnowflakeConfig.from_env(env)


def test_snowflake_config_reports_missing_variable():
    env = {k: v for k, v in SNOWFLAKE_ENV.items() if k != "SNOWFLAKE_ACCOUNT"}
    with pytest.raises(ConfigError, match="SNOWFLAKE_ACCOUNT"):
        SnowflakeConfig.from_env(env)
