"""Configuration read from environment variables, plus month-range handling."""

from __future__ import annotations

import calendar
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

_MONTH_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# Guard against accidentally loading years of data into a trial account.
MAX_MONTHS = 12


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True, order=True)
class Month:
    """A calendar month, the unit of work for trip and weather batches."""

    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> Month:
        match = _MONTH_RE.match(value.strip())
        if not match:
            raise ConfigError(f"Invalid month {value!r}; expected YYYY-MM, e.g. 2025-01")
        return cls(int(match.group(1)), int(match.group(2)))

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def last_day(self) -> date:
        return date(self.year, self.month, calendar.monthrange(self.year, self.month)[1])

    def next(self) -> Month:
        return Month(self.year + 1, 1) if self.month == 12 else Month(self.year, self.month + 1)


def month_range(start: Month, end: Month) -> list[Month]:
    """Return every month from start to end, inclusive."""
    if end < start:
        raise ConfigError(f"End month {end} is before start month {start}")
    months = [start]
    while months[-1] < end:
        months.append(months[-1].next())
        if len(months) > MAX_MONTHS:
            raise ConfigError(f"Range {start}..{end} exceeds the {MAX_MONTHS}-month limit")
    return months


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"Required environment variable {name} is not set")
    return value


def _identifier(env: Mapping[str, str], name: str, default: str | None = None) -> str:
    """Read a Snowflake object name; it is interpolated into SQL, so validate it."""
    value = env.get(name, "").strip() or default
    if not value:
        raise ConfigError(f"Required environment variable {name} is not set")
    if not _IDENTIFIER_RE.match(value):
        raise ConfigError(f"{name}={value!r} is not a valid unquoted Snowflake identifier")
    return value.upper()


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError as exc:
        raise ConfigError(f"{name}={raw!r} is not a number") from exc


@dataclass(frozen=True)
class PipelineConfig:
    """Settings needed by every step; none of them are secret."""

    data_dir: Path
    start: Month
    end: Month
    weather_latitude: float
    weather_longitude: float
    weather_timezone: str

    @property
    def months(self) -> list[Month]:
        return month_range(self.start, self.end)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> PipelineConfig:
        """Build from the environment; explicit start/end (CLI flags) take precedence."""
        env = os.environ if env is None else env
        config = cls(
            data_dir=Path(env.get("DATA_DIR", "").strip() or "data"),
            start=Month.parse(start or _require(env, "PIPELINE_START_MONTH")),
            end=Month.parse(end or _require(env, "PIPELINE_END_MONTH")),
            weather_latitude=_float(env, "WEATHER_LATITUDE", 40.7128),
            weather_longitude=_float(env, "WEATHER_LONGITUDE", -74.0060),
            weather_timezone=env.get("WEATHER_TIMEZONE", "").strip() or "America/New_York",
        )
        month_range(config.start, config.end)  # fail fast on an invalid range
        return config


@dataclass(frozen=True)
class SnowflakeConfig:
    """Snowflake connection settings. Only steps that touch the warehouse load this."""

    account: str
    user: str
    private_key_path: Path
    role: str
    warehouse: str
    database: str
    raw_schema: str
    private_key_passphrase: str | None = field(default=None, repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SnowflakeConfig:
        env = os.environ if env is None else env
        return cls(
            account=_require(env, "SNOWFLAKE_ACCOUNT"),
            user=_require(env, "SNOWFLAKE_USER"),
            private_key_path=Path(_require(env, "SNOWFLAKE_PRIVATE_KEY_PATH")),
            role=_identifier(env, "SNOWFLAKE_ROLE"),
            warehouse=_identifier(env, "SNOWFLAKE_WAREHOUSE"),
            database=_identifier(env, "SNOWFLAKE_DATABASE"),
            raw_schema=_identifier(env, "SNOWFLAKE_RAW_SCHEMA", default="RAW"),
            private_key_passphrase=env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip() or None,
        )
