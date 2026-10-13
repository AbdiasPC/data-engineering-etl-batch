import pyarrow as pa
import pytest

from src import sources
from src.config import Month
from src.sources import SchemaError

JAN = Month(2025, 1)


def trip_schema(**overrides: pa.DataType | None) -> pa.Schema:
    """The verified 2025 trip schema; pass name=type to change a column, name=None to drop."""
    fields: dict[str, pa.DataType] = {
        "VendorID": pa.int32(),
        "tpep_pickup_datetime": pa.timestamp("us"),
        "tpep_dropoff_datetime": pa.timestamp("us"),
        "passenger_count": pa.int64(),
        "trip_distance": pa.float64(),
        "RatecodeID": pa.int64(),
        "store_and_fwd_flag": pa.large_string(),
        "PULocationID": pa.int32(),
        "DOLocationID": pa.int32(),
        "payment_type": pa.int64(),
        "fare_amount": pa.float64(),
        "extra": pa.float64(),
        "mta_tax": pa.float64(),
        "tip_amount": pa.float64(),
        "tolls_amount": pa.float64(),
        "improvement_surcharge": pa.float64(),
        "total_amount": pa.float64(),
        "congestion_surcharge": pa.float64(),
        "Airport_fee": pa.float64(),
        "cbd_congestion_fee": pa.float64(),
    }
    fields.update(overrides)
    return pa.schema([(name, dtype) for name, dtype in fields.items() if dtype is not None])


def weather_payload(month: Month = JAN, days: int = 31) -> dict:
    dates = [f"{month}-{day:02d}" for day in range(1, days + 1)]
    daily = {name: [1.0] * days for name in sources.WEATHER_DAILY_VARIABLES}
    return {"daily": {"time": dates, **daily}}


# --- URL building -------------------------------------------------------------------


def test_trip_url_and_filename():
    assert sources.trip_filename(JAN) == "yellow_tripdata_2025-01.parquet"
    assert sources.trip_url(JAN) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    )


def test_weather_params_cover_the_whole_month():
    params = sources.weather_params(Month(2024, 2), 40.7128, -74.006, "America/New_York")
    assert params["start_date"] == "2024-02-01"
    assert params["end_date"] == "2024-02-29"
    assert params["daily"].split(",") == list(sources.WEATHER_DAILY_VARIABLES)
    assert params["timezone"] == "America/New_York"


def test_weather_filename():
    assert sources.weather_filename(JAN) == "weather_daily_2025-01.json"


# --- Trip schema contract -----------------------------------------------------------


def test_current_schema_is_valid_with_no_drift():
    check = sources.validate_trip_schema(trip_schema())
    assert check.notes == ()
    assert check.extra_columns == ()
    assert check.column_map["vendorid"] == "VendorID"
    assert check.column_map["airport_fee"] == "Airport_fee"
    assert list(check.column_map) == [spec.name for spec in sources.TRIP_COLUMNS]


def test_pre_2025_file_without_cbd_fee_is_tolerated():
    check = sources.validate_trip_schema(trip_schema(cbd_congestion_fee=None))
    assert check.column_map["cbd_congestion_fee"] is None
    assert any("cbd_congestion_fee" in note for note in check.notes)


def test_2023_style_names_and_types_are_tolerated():
    schema = trip_schema(
        Airport_fee=None,
        airport_fee=pa.float64(),
        passenger_count=pa.float64(),
        RatecodeID=pa.float64(),
        VendorID=pa.int64(),
    )
    check = sources.validate_trip_schema(schema)
    assert check.column_map["airport_fee"] == "airport_fee"


def test_unknown_column_is_reported_not_rejected():
    check = sources.validate_trip_schema(trip_schema(new_fee=pa.float64()))
    assert check.extra_columns == ("new_fee",)
    assert any("new_fee" in note for note in check.notes)


def test_missing_required_column_is_rejected():
    with pytest.raises(SchemaError, match="fare_amount"):
        sources.validate_trip_schema(trip_schema(fare_amount=None))


def test_changed_type_family_is_rejected():
    with pytest.raises(SchemaError, match="tpep_pickup_datetime"):
        sources.validate_trip_schema(trip_schema(tpep_pickup_datetime=pa.string()))
    with pytest.raises(SchemaError, match="fare_amount"):
        sources.validate_trip_schema(trip_schema(fare_amount=pa.string()))


def test_columns_differing_only_by_case_are_rejected():
    with pytest.raises(SchemaError, match="case"):
        sources.validate_trip_schema(trip_schema(airport_fee=pa.float64()))


# --- Zone and weather contracts -----------------------------------------------------


def test_zone_header_matches_case_insensitively():
    sources.validate_zone_header(["LocationID", "Borough", "Zone", "service_zone"])
    sources.validate_zone_header(["locationid", "borough", "zone", "SERVICE_ZONE"])


def test_zone_header_rejects_reordered_or_missing_columns():
    with pytest.raises(SchemaError):
        sources.validate_zone_header(["Borough", "LocationID", "Zone", "service_zone"])
    with pytest.raises(SchemaError):
        sources.validate_zone_header(["LocationID", "Borough", "Zone"])


def test_weather_payload_valid():
    assert sources.validate_weather_payload(weather_payload(), JAN) == 31


def test_weather_payload_rejects_missing_days():
    with pytest.raises(SchemaError, match="cover"):
        sources.validate_weather_payload(weather_payload(days=30), JAN)


def test_weather_payload_rejects_missing_variable():
    payload = weather_payload()
    del payload["daily"]["precipitation_sum"]
    with pytest.raises(SchemaError, match="precipitation_sum"):
        sources.validate_weather_payload(payload, JAN)


def test_weather_payload_rejects_error_response():
    with pytest.raises(SchemaError, match="daily"):
        sources.validate_weather_payload({"error": True, "reason": "bad date"}, JAN)
