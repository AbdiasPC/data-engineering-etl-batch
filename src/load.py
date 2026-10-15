"""Load: stage local files and COPY them into RAW, idempotently and with reconciliation.

Per file, inside one transaction:
    DELETE rows previously loaded from this file -> COPY INTO -> count rows for this file.
If the count differs from the local file's row count the transaction is rolled back, so a
bad load never replaces a good one. Every attempt is recorded in ETL_RUN_LOG.

A file whose checksum matches its last successful load, and whose rows are all still in
RAW, is skipped (logged as SKIPPED) unless a reload is forced. Either way the end state
is the same; skipping just avoids rewriting millions of unchanged rows.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from snowflake.connector import SnowflakeConnection

from src import extract, sources
from src.config import PipelineConfig, SnowflakeConfig
from src.extract import FileInfo
from src.snowflake_client import connect

logger = logging.getLogger(__name__)

RAW_OBJECTS_SQL = Path(__file__).resolve().parent.parent / "sql" / "01_raw_objects.sql"
STAGE = "LANDING"
RUN_LOG = "ETL_RUN_LOG"
METADATA_COLUMNS = ("_source_file", "_file_row_number", "_batch_id", "_loaded_at")


class ReconciliationError(RuntimeError):
    """RAW row count for a file does not match the source file."""


@dataclass(frozen=True)
class LoadJob:
    """Everything needed to load one local file into one RAW table."""

    info: FileInfo
    table: str
    file_format: str
    columns: tuple[str, ...]  # target columns, excluding metadata
    expressions: tuple[str, ...]  # one SELECT expression per target column
    schema_notes: str | None = None
    # (SQL aggregate over this file's RAW rows, value it must equal). Row counts alone
    # cannot catch a load that keeps every row but mangles values.
    value_checks: tuple[tuple[str, object], ...] = ()


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def stage_path(info: FileInfo) -> str:
    return f"@{STAGE}/{info.source}/{info.name}"


# --------------------------------------------------------------------------------------
# Building load jobs
# --------------------------------------------------------------------------------------


def trip_job(
    info: FileInfo,
    check: sources.TripSchemaCheck,
    pickup_range: tuple[datetime, datetime] | None = None,
) -> LoadJob:
    """Map this file's actual column names onto the canonical RAW columns.

    Parquet field names are case-sensitive in `$1:"name"`, so the names come from the
    file itself. Absent optional columns load as NULL; unknown columns are preserved in
    _EXTRA_COLUMNS rather than dropped.
    """
    expressions = []
    for spec in sources.TRIP_COLUMNS:
        actual = check.column_map[spec.name]
        source = f"$1:{_quoted(actual)}" if actual else "NULL"
        expressions.append(f"{source}::{spec.raw_type}")
    if check.extra_columns:
        pairs = ", ".join(f"{_literal(c)}, $1:{_quoted(c)}" for c in check.extra_columns)
        expressions.append(f"OBJECT_CONSTRUCT_KEEP_NULL({pairs})")
    else:
        expressions.append("NULL::VARIANT")
    return LoadJob(
        info=info,
        table="YELLOW_TRIPS",
        file_format="FF_PARQUET",
        columns=(*(spec.name for spec in sources.TRIP_COLUMNS), "_extra_columns"),
        expressions=tuple(expressions),
        schema_notes="; ".join(check.notes) or None,
        value_checks=(
            (
                ("MIN(tpep_pickup_datetime)", pickup_range[0]),
                ("MAX(tpep_pickup_datetime)", pickup_range[1]),
            )
            if pickup_range
            else ()
        ),
    )


def zone_job(info: FileInfo) -> LoadJob:
    return LoadJob(
        info=info,
        table="TAXI_ZONE_LOOKUP",
        file_format="FF_ZONES_CSV",
        columns=("locationid", "borough", "zone", "service_zone"),
        expressions=("$1", "$2", "$3", "$4"),
    )


def weather_job(info: FileInfo) -> LoadJob:
    return LoadJob(
        info=info,
        table="WEATHER_DAILY_JSON",
        file_format="FF_JSON",
        columns=("payload",),
        expressions=("$1",),
    )


def copy_statement(job: LoadJob, batch_id: str) -> str:
    """COPY with an explicit column mapping plus the four metadata columns."""
    columns = ", ".join((*job.columns, *METADATA_COLUMNS))
    metadata = (
        _literal(job.info.name),
        "METADATA$FILE_ROW_NUMBER",
        _literal(batch_id),
        "METADATA$START_SCAN_TIME",
    )
    select_list = ",\n        ".join((*job.expressions, *metadata))
    return (
        f"COPY INTO {job.table} ({columns})\n"
        f"FROM (\n    SELECT\n        {select_list}\n    FROM {stage_path(job.info)}\n)\n"
        f"FILE_FORMAT = (FORMAT_NAME = '{job.file_format}')\n"
        # FORCE: Snowflake's load history would otherwise skip a file it has seen before,
        # leaving the table empty after our DELETE. ABORT_STATEMENT: all rows or none.
        "FORCE = TRUE ON_ERROR = ABORT_STATEMENT"
    )


def build_jobs(config: PipelineConfig) -> list[LoadJob]:
    """Re-validate every expected local file and plan its load. Fails if any is missing."""
    data_dir = config.data_dir
    zones = sources.local_path(data_dir, sources.ZONES, sources.ZONE_LOOKUP_FILENAME)
    expected = [zones]
    for month in config.months:
        expected.append(sources.local_path(data_dir, sources.TRIPS, sources.trip_filename(month)))
        expected.append(
            sources.local_path(data_dir, sources.WEATHER, sources.weather_filename(month))
        )
    missing = [str(path) for path in expected if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Source files not found (run extract first): {missing}")

    jobs = [zone_job(extract.inspect_zones(zones))]
    for month in config.months:
        trips = sources.local_path(data_dir, sources.TRIPS, sources.trip_filename(month))
        info, check = extract.inspect_trips(trips)
        pickup_column = check.column_map["tpep_pickup_datetime"]
        jobs.append(trip_job(info, check, extract.column_range(trips, pickup_column)))
        weather = sources.local_path(data_dir, sources.WEATHER, sources.weather_filename(month))
        jobs.append(weather_job(extract.inspect_weather(weather, month)))
    return jobs


# --------------------------------------------------------------------------------------
# Executing against Snowflake
# --------------------------------------------------------------------------------------


def _value(connection: SnowflakeConnection, sql: str, params: Sequence | None = None) -> object:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()[0]


def _scalar(connection: SnowflakeConnection, sql: str, params: Sequence | None = None) -> int:
    return int(_value(connection, sql, params))


def _execute(connection: SnowflakeConnection, sql: str, params: Sequence | None = None) -> int:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.rowcount or 0


def ensure_raw_objects(connection: SnowflakeConnection) -> None:
    connection.execute_string(RAW_OBJECTS_SQL.read_text(encoding="utf-8"))


def _log_start(
    connection: SnowflakeConnection, job: LoadJob, batch_id: str, status: str = "STARTED"
) -> None:
    info = job.info
    _execute(
        connection,
        f"INSERT INTO {RUN_LOG} (batch_id, source_name, source_file, target_table, status, "
        "source_row_count, file_size_bytes, file_sha256, schema_notes, started_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())",
        (
            batch_id,
            info.source,
            info.name,
            job.table,
            status,
            info.row_count,
            info.size_bytes,
            info.sha256,
            job.schema_notes,
        ),
    )


def _log_end(
    connection: SnowflakeConnection,
    job: LoadJob,
    batch_id: str,
    status: str,
    loaded_rows: int | None,
    error: str | None,
) -> None:
    _execute(
        connection,
        f"UPDATE {RUN_LOG} SET status = %s, loaded_row_count = %s, error_message = %s, "
        "ended_at = CURRENT_TIMESTAMP() WHERE batch_id = %s AND source_file = %s",
        (status, loaded_rows, error, batch_id, job.info.name),
    )


def _replace_and_reconcile(connection: SnowflakeConnection, job: LoadJob, batch_id: str) -> int:
    """Atomically replace this file's rows in RAW. Returns the reconciled row count."""
    file_name = job.info.name
    _execute(connection, "BEGIN")
    try:
        replaced = _execute(
            connection, f"DELETE FROM {job.table} WHERE _source_file = %s", (file_name,)
        )
        _execute(connection, copy_statement(job, batch_id))
        loaded = _rows_in_raw(connection, job)
        if loaded != job.info.row_count:
            raise ReconciliationError(
                f"{file_name}: source has {job.info.row_count} rows, RAW has {loaded}"
            )
        for expression, expected in job.value_checks:
            actual = _value(
                connection,
                f"SELECT {expression} FROM {job.table} WHERE _source_file = %s",
                (file_name,),
            )
            if actual != expected:
                raise ReconciliationError(
                    f"{file_name}: {expression} is {actual} in RAW but {expected} in the source"
                )
        _execute(connection, "COMMIT")
    except BaseException:
        _execute(connection, "ROLLBACK")
        raise
    logger.info(
        "Loaded and reconciled",
        extra={
            "file": file_name,
            "table": job.table,
            "source_rows": job.info.row_count,
            "loaded_rows": loaded,
            "rows_replaced": replaced,
        },
    )
    return loaded


def _rows_in_raw(connection: SnowflakeConnection, job: LoadJob) -> int:
    return _scalar(
        connection, f"SELECT COUNT(*) FROM {job.table} WHERE _source_file = %s", (job.info.name,)
    )


def is_already_loaded(connection: SnowflakeConnection, job: LoadJob) -> bool:
    """True if this exact file (by sha256) was loaded successfully and RAW still has it all."""
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT file_sha256 FROM {RUN_LOG} WHERE source_file = %s AND status = 'SUCCESS' "
            "ORDER BY ended_at DESC LIMIT 1",
            (job.info.name,),
        )
        row = cursor.fetchone()
    if row is None or row[0] != job.info.sha256:
        return False
    return _rows_in_raw(connection, job) == job.info.row_count


def load_file(
    connection: SnowflakeConnection, job: LoadJob, batch_id: str, force: bool = False
) -> None:
    """Stage, load and reconcile one file, recording the outcome in the run log."""
    info = job.info
    if not force and is_already_loaded(connection, job):
        _log_start(connection, job, batch_id, status="SKIPPED")
        _log_end(connection, job, batch_id, "SKIPPED", info.row_count, None)
        logger.info("Unchanged file already in RAW; skipped", extra={"file": info.name})
        return
    _log_start(connection, job, batch_id)
    try:
        _execute(
            connection,
            f"PUT {_literal(info.path.resolve().as_uri())} @{STAGE}/{info.source} "
            "AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
        )
        loaded = _replace_and_reconcile(connection, job, batch_id)
    except Exception as exc:
        _log_end(connection, job, batch_id, "FAILED", None, str(exc)[:2000])
        raise
    _log_end(connection, job, batch_id, "SUCCESS", loaded, None)
    # The staged copy has served its purpose; the local file remains the replay source.
    _execute(connection, f"REMOVE {stage_path(info)}")


def run_load(config: PipelineConfig, snowflake_config: SnowflakeConfig) -> str:
    """Load every file for the configured months. Stops at the first failure."""
    jobs = build_jobs(config)
    batch_id = uuid.uuid4().hex
    logger.info("Load starting", extra={"batch_id": batch_id, "files": len(jobs)})
    with connect(snowflake_config) as connection:
        ensure_raw_objects(connection)
        for job in jobs:
            load_file(connection, job, batch_id, force=config.force_reload)
    logger.info("Load complete", extra={"batch_id": batch_id, "files": len(jobs)})
    return batch_id
