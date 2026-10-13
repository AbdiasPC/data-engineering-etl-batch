"""Source definitions: URLs, file names, and the schema contracts each file must meet.

URLs and column names were verified against the live sources (see README, "Data sources").
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from src.config import Month

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_LOOKUP_URL = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"
ZONE_LOOKUP_FILENAME = "taxi_zone_lookup.csv"
WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Source names double as local sub-directories, stage prefixes and run-log labels.
TRIPS = "yellow_trips"
ZONES = "taxi_zones"
WEATHER = "weather_daily"

ZONE_LOOKUP_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")

WEATHER_DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
)


class SchemaError(ValueError):
    """A source file does not meet its contract and must not be loaded."""


def trip_filename(month: Month) -> str:
    return f"yellow_tripdata_{month}.parquet"


def trip_url(month: Month) -> str:
    return f"{TLC_BASE_URL}/trip-data/{trip_filename(month)}"


def weather_filename(month: Month) -> str:
    return f"weather_daily_{month}.json"


def weather_params(month: Month, latitude: float, longitude: float, timezone: str) -> dict:
    """Query parameters for one month of daily weather."""
    return {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": month.first_day.isoformat(),
        "end_date": month.last_day.isoformat(),
        "daily": ",".join(WEATHER_DAILY_VARIABLES),
        "timezone": timezone,
    }


def local_path(data_dir: Path, source: str, filename: str) -> Path:
    return data_dir / "raw" / source / filename


# --------------------------------------------------------------------------------------
# Yellow trips contract
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnSpec:
    """One canonical RAW column and what a source file must provide for it."""

    name: str  # canonical lower-case name; source names match case-insensitively
    kind: str  # "numeric" | "timestamp" | "string"
    raw_type: str  # Snowflake type in RAW, the widest type seen across monthly files
    required: bool = True


# TLC has changed integer widths and int/double between months (e.g. passenger_count was
# DOUBLE in 2023 and INT64 in 2025), so every numeric column accepts any numeric type and
# RAW stores the widest one. Staging casts to the final business types.
TRIP_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("vendorid", "numeric", "NUMBER(38,0)"),
    ColumnSpec("tpep_pickup_datetime", "timestamp", "TIMESTAMP_NTZ"),
    ColumnSpec("tpep_dropoff_datetime", "timestamp", "TIMESTAMP_NTZ"),
    ColumnSpec("passenger_count", "numeric", "FLOAT"),
    ColumnSpec("trip_distance", "numeric", "FLOAT"),
    ColumnSpec("ratecodeid", "numeric", "FLOAT"),
    ColumnSpec("store_and_fwd_flag", "string", "VARCHAR"),
    ColumnSpec("pulocationid", "numeric", "NUMBER(38,0)"),
    ColumnSpec("dolocationid", "numeric", "NUMBER(38,0)"),
    ColumnSpec("payment_type", "numeric", "NUMBER(38,0)"),
    ColumnSpec("fare_amount", "numeric", "FLOAT"),
    ColumnSpec("extra", "numeric", "FLOAT"),
    ColumnSpec("mta_tax", "numeric", "FLOAT"),
    ColumnSpec("tip_amount", "numeric", "FLOAT"),
    ColumnSpec("tolls_amount", "numeric", "FLOAT"),
    ColumnSpec("improvement_surcharge", "numeric", "FLOAT"),
    ColumnSpec("total_amount", "numeric", "FLOAT"),
    ColumnSpec("congestion_surcharge", "numeric", "FLOAT"),
    ColumnSpec("airport_fee", "numeric", "FLOAT"),  # "Airport_fee" in 2024+ files
    ColumnSpec("cbd_congestion_fee", "numeric", "FLOAT", required=False),  # added 2025-01
)


@dataclass(frozen=True)
class TripSchemaCheck:
    """How one Parquet file maps onto the contract."""

    column_map: dict[str, str | None]  # canonical name -> name in this file (None = absent)
    extra_columns: tuple[str, ...]  # columns in the file that the contract does not know
    notes: tuple[str, ...]  # human-readable drift notes for the run log


def _kind_of(data_type: pa.DataType) -> str:
    if pa.types.is_integer(data_type) or pa.types.is_floating(data_type):
        return "numeric"
    if pa.types.is_decimal(data_type):
        return "numeric"
    if pa.types.is_timestamp(data_type):
        return "timestamp"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "string"
    return str(data_type)


def validate_trip_schema(schema: pa.Schema) -> TripSchemaCheck:
    """Check a trip file's schema against the contract and describe any drift.

    Raises SchemaError for breaking drift (required column missing, or a column whose
    type family changed). Tolerates and reports non-breaking drift (optional column
    missing, unknown new columns, name-case changes).
    """
    by_lower: dict[str, pa.Field] = {}
    for field in schema:
        if field.name.lower() in by_lower:
            raise SchemaError(f"Columns differ only by case: {field.name!r}")
        by_lower[field.name.lower()] = field

    column_map: dict[str, str | None] = {}
    notes: list[str] = []
    problems: list[str] = []
    for spec in TRIP_COLUMNS:
        field = by_lower.pop(spec.name, None)
        if field is None:
            column_map[spec.name] = None
            if spec.required:
                problems.append(f"required column {spec.name!r} is missing")
            else:
                notes.append(f"optional column {spec.name!r} absent; loaded as NULL")
            continue
        column_map[spec.name] = field.name
        actual_kind = _kind_of(field.type)
        if actual_kind != spec.kind:
            problems.append(f"column {field.name!r} is {field.type}, expected {spec.kind}")

    if problems:
        raise SchemaError("; ".join(problems))

    extra = tuple(field.name for field in by_lower.values())
    if extra:
        notes.append(f"unknown columns kept in _EXTRA_COLUMNS: {', '.join(extra)}")
    return TripSchemaCheck(column_map=column_map, extra_columns=extra, notes=tuple(notes))


# --------------------------------------------------------------------------------------
# Zone lookup and weather contracts
# --------------------------------------------------------------------------------------


def validate_zone_header(header: list[str]) -> None:
    """The zone CSV is loaded positionally, so its header must match exactly."""
    if [h.strip().lower() for h in header] != [c.lower() for c in ZONE_LOOKUP_COLUMNS]:
        raise SchemaError(f"Zone lookup header is {header}, expected {list(ZONE_LOOKUP_COLUMNS)}")


def validate_weather_payload(payload: Mapping[str, Any], month: Month) -> int:
    """Check an Open-Meteo response covers every day of the month; return the day count."""
    daily = payload.get("daily")
    if not isinstance(daily, Mapping):
        raise SchemaError("Weather response has no 'daily' object")
    missing = [name for name in ("time", *WEATHER_DAILY_VARIABLES) if name not in daily]
    if missing:
        raise SchemaError(f"Weather response is missing daily fields: {missing}")

    expected_days = _days_in(month)
    if list(daily["time"]) != [d.isoformat() for d in expected_days]:
        raise SchemaError(f"Weather dates do not cover {month} exactly")
    wrong_length = [
        name for name in WEATHER_DAILY_VARIABLES if len(daily[name]) != len(expected_days)
    ]
    if wrong_length:
        raise SchemaError(f"Weather arrays have the wrong length: {wrong_length}")
    return len(expected_days)


def _days_in(month: Month) -> list[date]:
    count = (month.last_day - month.first_day).days + 1
    return [month.first_day + timedelta(days=offset) for offset in range(count)]
