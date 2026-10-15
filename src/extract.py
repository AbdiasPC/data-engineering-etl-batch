"""Extract: download source files unchanged to the local data directory and validate them."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from src import sources
from src.config import Month, PipelineConfig

logger = logging.getLogger(__name__)

USER_AGENT = "nyc-taxi-elt-portfolio/1.0 (python-requests)"
CHUNK_BYTES = 1024 * 1024
CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 120
MAX_ATTEMPTS = 5
BACKOFF_BASE_S = 2.0
RETRY_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class SourceUnavailableError(RuntimeError):
    """A source returned a non-retryable error (e.g. the month is not published yet)."""


class _RetryableError(RuntimeError):
    """A transient failure worth another attempt."""


# ChunkedEncodingError is what a connection dropped mid-body surfaces as.
_TRANSIENT_ERRORS = (
    _RetryableError,
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


@dataclass(frozen=True)
class FileInfo:
    """A local source file and the facts the load step reconciles against."""

    source: str
    path: Path
    size_bytes: int
    sha256: str
    row_count: int

    @property
    def name(self) -> str:
        return self.path.name


def with_retries[T](
    func: Callable[[], T],
    *,
    attempts: int = MAX_ATTEMPTS,
    base_delay: float = BACKOFF_BASE_S,
    sleep: Callable[[float], None] | None = None,
) -> T:
    """Call func, retrying transient failures with exponential backoff (2s, 4s, 8s, ...)."""
    sleep = sleep or time.sleep
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except _TRANSIENT_ERRORS as exc:
            if attempt == attempts:
                raise SourceUnavailableError(f"Gave up after {attempts} attempts: {exc}") from exc
            delay = base_delay * 2 ** (attempt - 1)
            logger.warning(
                "Transient download failure; retrying",
                extra={"attempt": attempt, "retry_in_s": delay, "error": str(exc)},
            )
            sleep(delay)
    raise AssertionError("unreachable")


def _check_status(response: requests.Response) -> None:
    if response.status_code in RETRY_STATUS_CODES:
        raise _RetryableError(f"HTTP {response.status_code} from {response.url}")
    if response.status_code >= 400:
        raise SourceUnavailableError(
            f"HTTP {response.status_code} from {response.url}: {response.text[:200]!r}"
        )


def _download_once(
    session: requests.Session, url: str, dest: Path, params: dict[str, Any] | None
) -> None:
    """Stream url to dest via a temporary file, so a partial download never looks complete."""
    partial = dest.with_name(dest.name + ".part")
    with session.get(
        url, params=params, stream=True, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S)
    ) as response:
        _check_status(response)
        # A body shorter than Content-Length raises ChunkedEncodingError, which is retried.
        with partial.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_BYTES):
                handle.write(chunk)
    partial.replace(dest)


def download(
    session: requests.Session, url: str, dest: Path, params: dict[str, Any] | None = None
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with_retries(lambda: _download_once(session, url, dest, params))


def remote_size(session: requests.Session, url: str) -> int | None:
    """Content-Length from a HEAD request, or None if it cannot be determined."""
    try:
        response = session.head(url, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
    except requests.RequestException:
        return None
    length = response.headers.get("Content-Length")
    return int(length) if response.ok and length is not None else None


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_info(source: str, path: Path, row_count: int) -> FileInfo:
    info = FileInfo(source, path, path.stat().st_size, sha256_of(path), row_count)
    logger.info(
        "Source file ready",
        extra={
            "source": source,
            "file": info.name,
            "size_bytes": info.size_bytes,
            "sha256": info.sha256,
            "row_count": row_count,
        },
    )
    return info


# --------------------------------------------------------------------------------------
# Inspecting local files (also used by the load step, which trusts nothing but the file)
# --------------------------------------------------------------------------------------


def inspect_trips(path: Path) -> tuple[FileInfo, sources.TripSchemaCheck]:
    """Validate a trip file against the contract; row count comes from the Parquet footer."""
    parquet = pq.ParquetFile(path)
    check = sources.validate_trip_schema(parquet.schema_arrow)
    return _file_info(sources.TRIPS, path, parquet.metadata.num_rows), check


def column_range(path: Path, column: str) -> tuple[Any, Any]:
    """Min and max of one Parquet column, read without loading the rest of the file."""
    values = pq.read_table(path, columns=[column]).column(column)
    result = pc.min_max(values)
    return result["min"].as_py(), result["max"].as_py()


def inspect_zones(path: Path) -> FileInfo:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        sources.validate_zone_header(next(reader, []))
        row_count = sum(1 for row in reader if row)
    return _file_info(sources.ZONES, path, row_count)


def inspect_weather(path: Path, month: Month) -> FileInfo:
    """One JSON document per file, so the loadable row count is 1."""
    days = sources.validate_weather_payload(json.loads(path.read_text(encoding="utf-8")), month)
    logger.info("Weather file covers %d days", days, extra={"file": path.name, "month": str(month)})
    return _file_info(sources.WEATHER, path, 1)


# --------------------------------------------------------------------------------------
# Extract steps
# --------------------------------------------------------------------------------------


def extract_trips(config: PipelineConfig, session: requests.Session, month: Month) -> FileInfo:
    """Download one month of trips unless an identical-size copy is already on disk."""
    url = sources.trip_url(month)
    dest = sources.local_path(config.data_dir, sources.TRIPS, sources.trip_filename(month))
    if dest.exists() and dest.stat().st_size == remote_size(session, url):
        logger.info("Local copy matches remote size; skipping download", extra={"file": dest.name})
    else:
        logger.info("Downloading", extra={"url": url})
        download(session, url, dest)
    return inspect_trips(dest)[0]


def extract_zones(config: PipelineConfig, session: requests.Session) -> FileInfo:
    dest = sources.local_path(config.data_dir, sources.ZONES, sources.ZONE_LOOKUP_FILENAME)
    logger.info("Downloading", extra={"url": sources.ZONE_LOOKUP_URL})
    download(session, sources.ZONE_LOOKUP_URL, dest)
    return inspect_zones(dest)


def extract_weather(config: PipelineConfig, session: requests.Session, month: Month) -> FileInfo:
    dest = sources.local_path(config.data_dir, sources.WEATHER, sources.weather_filename(month))
    params = sources.weather_params(
        month, config.weather_latitude, config.weather_longitude, config.weather_timezone
    )
    logger.info("Downloading", extra={"url": sources.WEATHER_ARCHIVE_URL, "month": str(month)})
    download(session, sources.WEATHER_ARCHIVE_URL, dest, params=params)
    return inspect_weather(dest, month)


def run_extract(config: PipelineConfig) -> list[FileInfo]:
    """Download and validate every source file for the configured month range."""
    files: list[FileInfo] = []
    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        files.append(extract_zones(config, session))
        for month in config.months:
            files.append(extract_trips(config, session, month))
            files.append(extract_weather(config, session, month))
    logger.info("Extract complete", extra={"files": len(files)})
    return files
