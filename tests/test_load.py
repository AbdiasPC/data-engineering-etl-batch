import re
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest

from src import load, sources
from src.extract import FileInfo
from tests.test_sources import trip_schema

BATCH = "batch123"


def file_info(source: str, name: str, rows: int) -> FileInfo:
    return FileInfo(source, Path("/app/data/raw") / source / name, 100, "abc", rows)


def trips_job(schema: pa.Schema | None = None, rows: int = 10) -> load.LoadJob:
    info = file_info(sources.TRIPS, "yellow_tripdata_2025-01.parquet", rows)
    return load.trip_job(info, sources.validate_trip_schema(schema or trip_schema()))


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.rowcount = 0
        self._result: tuple | None = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.statements.append(sql)
        self.connection.params.append(params)
        if sql.startswith("COPY") and self.connection.copy_error:
            raise self.connection.copy_error
        if sql.startswith("SELECT COUNT"):
            self._result = (self.connection.loaded_count,)
        elif sql.startswith(("SELECT MIN", "SELECT MAX")):
            self._result = (self.connection.raw_value,)
        elif sql.startswith("SELECT file_sha256"):
            previous = self.connection.previous_sha256
            self._result = (previous,) if previous else None

    def fetchone(self) -> tuple | None:
        return self._result


class FakeConnection:
    """Records statements; reports a configurable post-COPY row count."""

    def __init__(
        self,
        loaded_count: int,
        copy_error: Exception | None = None,
        previous_sha256: str | None = None,
        raw_value: object = None,
    ) -> None:
        self.raw_value = raw_value
        self.loaded_count = loaded_count
        self.copy_error = copy_error
        self.previous_sha256 = previous_sha256
        self.statements: list[str] = []
        self.params: list[object] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def verbs(self) -> list[str]:
        return [statement.split()[0] for statement in self.statements]


# --- SQL generation -----------------------------------------------------------------


def test_raw_ddl_matches_the_trip_contract():
    ddl = load.RAW_OBJECTS_SQL.read_text()
    table = re.search(r"CREATE TABLE IF NOT EXISTS YELLOW_TRIPS \((.*?)\n\);", ddl, re.S).group(1)
    declared = dict(re.findall(r"^\s+(\w+)\s+([A-Z_]+(?:\(\d+,\d+\))?)", table, re.M))
    for spec in sources.TRIP_COLUMNS:
        assert declared[spec.name.upper()] == spec.raw_type


def test_trip_copy_uses_the_files_exact_column_names():
    sql = load.copy_statement(trips_job(), BATCH)
    assert '$1:"VendorID"::NUMBER(38,0)' in sql
    assert '$1:"Airport_fee"::FLOAT' in sql
    assert "FROM @LANDING/yellow_trips/yellow_tripdata_2025-01.parquet" in sql
    assert "FORCE = TRUE ON_ERROR = ABORT_STATEMENT" in sql


def test_trip_copy_adds_metadata_columns():
    sql = load.copy_statement(trips_job(), BATCH)
    assert "_source_file, _file_row_number, _batch_id, _loaded_at)" in sql
    assert "'yellow_tripdata_2025-01.parquet'" in sql
    assert "METADATA$FILE_ROW_NUMBER" in sql
    assert f"'{BATCH}'" in sql


def test_trip_copy_selects_one_expression_per_target_column():
    job = trips_job()
    assert len(job.columns) == len(job.expressions) == len(sources.TRIP_COLUMNS) + 1


def test_missing_optional_column_loads_as_null_and_is_noted():
    job = trips_job(trip_schema(cbd_congestion_fee=None))
    assert job.expressions[job.columns.index("cbd_congestion_fee")] == "NULL::FLOAT"
    assert "cbd_congestion_fee" in job.schema_notes


def test_unknown_column_is_preserved_in_extra_columns():
    job = trips_job(trip_schema(new_fee=pa.float64()))
    assert job.expressions[-1] == "OBJECT_CONSTRUCT_KEEP_NULL('new_fee', $1:\"new_fee\")"


def test_no_drift_means_null_extras_and_no_notes():
    job = trips_job()
    assert job.expressions[-1] == "NULL::VARIANT"
    assert job.schema_notes is None


def test_zone_and_weather_copy_statements():
    zones = load.copy_statement(
        load.zone_job(file_info(sources.ZONES, "taxi_zone_lookup.csv", 265)), BATCH
    )
    assert "COPY INTO TAXI_ZONE_LOOKUP (locationid, borough, zone, service_zone, _source" in zones
    assert "FORMAT_NAME = 'FF_ZONES_CSV'" in zones
    weather = load.copy_statement(
        load.weather_job(file_info(sources.WEATHER, "weather_daily_2025-01.json", 1)), BATCH
    )
    assert "COPY INTO WEATHER_DAILY_JSON (payload, _source_file" in weather
    assert "FROM @LANDING/weather_daily/weather_daily_2025-01.json" in weather


def test_literal_escapes_quotes():
    assert load._literal("it's") == "'it''s'"


# --- Load behaviour -----------------------------------------------------------------


def test_load_file_deletes_then_copies_in_one_transaction():
    connection = FakeConnection(loaded_count=10)
    load.load_file(connection, trips_job(rows=10), BATCH)
    assert connection.verbs() == [
        "SELECT",  # was this exact file already loaded?
        "INSERT",  # run log STARTED
        "PUT",
        "BEGIN",
        "DELETE",
        "COPY",
        "SELECT",
        "COMMIT",
        "UPDATE",  # run log SUCCESS
        "REMOVE",
    ]
    delete_index = connection.verbs().index("DELETE")
    assert connection.params[delete_index] == ("yellow_tripdata_2025-01.parquet",)
    assert connection.params[-2][:3] == ("SUCCESS", 10, None)


def test_unchanged_file_is_skipped_and_logged():
    connection = FakeConnection(loaded_count=10, previous_sha256="abc")
    load.load_file(connection, trips_job(rows=10), BATCH)
    assert connection.verbs() == ["SELECT", "SELECT", "INSERT", "UPDATE"]
    assert connection.params[-1][:2] == ("SKIPPED", 10)


def test_force_reloads_an_unchanged_file():
    connection = FakeConnection(loaded_count=10, previous_sha256="abc")
    load.load_file(connection, trips_job(rows=10), BATCH, force=True)
    assert "COPY" in connection.verbs()
    assert connection.verbs()[0] == "INSERT"


def test_changed_checksum_triggers_reload():
    connection = FakeConnection(loaded_count=10, previous_sha256="different")
    load.load_file(connection, trips_job(rows=10), BATCH)
    assert "COPY" in connection.verbs()


def test_unchanged_file_with_rows_missing_from_raw_is_reloaded():
    connection = FakeConnection(loaded_count=10, previous_sha256="abc")
    connection.loaded_count = 7  # RAW lost rows since the last successful load
    with pytest.raises(load.ReconciliationError):
        load.load_file(connection, trips_job(rows=10), BATCH)
    assert "COPY" in connection.verbs()


def test_trip_job_checks_pickup_range_when_given():
    low, high = datetime(2025, 1, 1, 0, 0, 1), datetime(2025, 1, 31, 23, 59)
    info = file_info(sources.TRIPS, "yellow_tripdata_2025-01.parquet", 10)
    job = load.trip_job(info, sources.validate_trip_schema(trip_schema()), (low, high))
    assert job.value_checks == (
        ("MIN(tpep_pickup_datetime)", low),
        ("MAX(tpep_pickup_datetime)", high),
    )


def test_mangled_values_roll_back_even_when_row_counts_match():
    """The real failure this guards against: timestamps loaded as seconds, not microseconds."""
    expected = datetime(2025, 1, 1)
    job = load.LoadJob(
        info=file_info(sources.TRIPS, "yellow_tripdata_2025-01.parquet", 10),
        table="YELLOW_TRIPS",
        file_format="FF_PARQUET",
        columns=("tpep_pickup_datetime",),
        expressions=("$1",),
        value_checks=(("MIN(tpep_pickup_datetime)", expected),),
    )
    connection = FakeConnection(loaded_count=10, raw_value=datetime(9999, 1, 1))
    with pytest.raises(load.ReconciliationError, match="MIN\\(tpep_pickup_datetime\\)"):
        load.load_file(connection, job, BATCH)
    assert "COMMIT" not in connection.verbs()
    assert connection.verbs()[-2:] == ["ROLLBACK", "UPDATE"]


def test_matching_values_commit():
    expected = datetime(2025, 1, 1)
    job = load.LoadJob(
        info=file_info(sources.TRIPS, "yellow_tripdata_2025-01.parquet", 10),
        table="YELLOW_TRIPS",
        file_format="FF_PARQUET",
        columns=("tpep_pickup_datetime",),
        expressions=("$1",),
        value_checks=(("MIN(tpep_pickup_datetime)", expected),),
    )
    connection = FakeConnection(loaded_count=10, raw_value=expected)
    load.load_file(connection, job, BATCH)
    assert "COMMIT" in connection.verbs()


def test_row_count_mismatch_rolls_back_and_logs_failure():
    connection = FakeConnection(loaded_count=9)
    with pytest.raises(load.ReconciliationError, match="source has 10 rows, RAW has 9"):
        load.load_file(connection, trips_job(rows=10), BATCH)
    assert "COMMIT" not in connection.verbs()
    assert connection.verbs()[-2:] == ["ROLLBACK", "UPDATE"]
    status, loaded, error = connection.params[-1][:3]
    assert (status, loaded) == ("FAILED", None)
    assert "RAW has 9" in error


def test_copy_error_rolls_back_and_logs_failure():
    connection = FakeConnection(loaded_count=0, copy_error=RuntimeError("bad file"))
    with pytest.raises(RuntimeError, match="bad file"):
        load.load_file(connection, trips_job(), BATCH)
    assert connection.verbs()[-2:] == ["ROLLBACK", "UPDATE"]
    assert connection.params[-1][0] == "FAILED"


def test_build_jobs_requires_extracted_files(tmp_path):
    from src.config import Month, PipelineConfig

    config = PipelineConfig(tmp_path, Month(2025, 1), Month(2025, 1), 40.7, -74.0, "UTC")
    with pytest.raises(FileNotFoundError, match="run extract first"):
        load.build_jobs(config)
