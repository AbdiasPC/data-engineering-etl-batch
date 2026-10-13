import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import requests
import responses

from src import extract, sources
from src.config import Month, PipelineConfig
from src.extract import SourceUnavailableError
from tests.test_sources import trip_schema, weather_payload

JAN = Month(2025, 1)
URL = "https://example.test/file.bin"
ZONE_CSV = (
    '"LocationID","Borough","Zone","service_zone"\n'
    '1,"EWR","Newark Airport","EWR"\n'
    '264,"Unknown","N/A","N/A"\n'
)


@pytest.fixture
def config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(tmp_path, JAN, JAN, 40.7128, -74.006, "America/New_York")


@pytest.fixture
def session() -> requests.Session:
    return requests.Session()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record backoff delays instead of sleeping."""
    delays: list[float] = []
    monkeypatch.setattr(extract.time, "sleep", delays.append)
    return delays


def parquet_bytes(tmp_path: Path, rows: int = 3) -> bytes:
    path = tmp_path / "source.parquet"
    table = trip_schema().empty_table()
    pq.write_table(table.from_pylist([{}] * rows, schema=table.schema), path)
    return path.read_bytes()


# --- with_retries -------------------------------------------------------------------


def test_with_retries_backs_off_exponentially_then_succeeds():
    delays: list[float] = []
    outcomes = iter([requests.ConnectionError("a"), requests.Timeout("b"), "ok"])

    def flaky() -> str:
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    assert extract.with_retries(flaky, base_delay=2.0, sleep=delays.append) == "ok"
    assert delays == [2.0, 4.0]


def test_with_retries_gives_up_after_max_attempts():
    calls: list[int] = []

    def always_fails() -> None:
        calls.append(1)
        raise requests.ConnectionError("down")

    with pytest.raises(SourceUnavailableError, match="3 attempts"):
        extract.with_retries(always_fails, attempts=3, sleep=lambda _: None)
    assert len(calls) == 3


# --- download -----------------------------------------------------------------------


@responses.activate
def test_download_writes_file_and_leaves_no_partial(tmp_path, session):
    responses.get(URL, body=b"hello world")
    dest = tmp_path / "nested" / "file.bin"
    extract.download(session, URL, dest)
    assert dest.read_bytes() == b"hello world"
    assert list(dest.parent.iterdir()) == [dest]


@responses.activate
def test_download_retries_server_errors(tmp_path, session, no_sleep):
    responses.get(URL, status=503)
    responses.get(URL, status=429)
    responses.get(URL, body=b"ok")
    extract.download(session, URL, tmp_path / "file.bin")
    assert len(responses.calls) == 3
    assert no_sleep == [2.0, 4.0]


@responses.activate
def test_download_does_not_retry_client_errors(tmp_path, session):
    responses.get(URL, status=403, body="AccessDenied")
    dest = tmp_path / "file.bin"
    with pytest.raises(SourceUnavailableError, match="403"):
        extract.download(session, URL, dest)
    assert len(responses.calls) == 1
    assert not dest.exists()


@responses.activate
def test_download_retries_then_fails_on_truncated_body(tmp_path, session):
    responses.get(
        URL, body=b"short", headers={"Content-Length": "999"}, auto_calculate_content_length=False
    )
    dest = tmp_path / "file.bin"
    with pytest.raises(SourceUnavailableError, match="5 attempts"):
        extract.download(session, URL, dest)
    assert len(responses.calls) == 5
    assert not dest.exists()


def test_sha256_of(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"abc")
    assert extract.sha256_of(path) == hashlib.sha256(b"abc").hexdigest()


# --- extract steps ------------------------------------------------------------------


@responses.activate
def test_extract_zones_counts_data_rows(config, session):
    responses.get(sources.ZONE_LOOKUP_URL, body=ZONE_CSV)
    info = extract.extract_zones(config, session)
    assert info.row_count == 2
    assert info.source == sources.ZONES
    assert info.path.read_text() == ZONE_CSV  # stored unmodified


@responses.activate
def test_extract_trips_reads_row_count_from_footer(config, session, tmp_path):
    body = parquet_bytes(tmp_path, rows=3)
    responses.get(sources.trip_url(JAN), body=body)
    info = extract.extract_trips(config, session, JAN)
    assert info.row_count == 3
    assert info.size_bytes == len(body)
    assert info.sha256 == hashlib.sha256(body).hexdigest()


@responses.activate
def test_extract_trips_skips_download_when_size_matches(config, session, tmp_path):
    body = parquet_bytes(tmp_path)
    dest = sources.local_path(config.data_dir, sources.TRIPS, sources.trip_filename(JAN))
    dest.parent.mkdir(parents=True)
    dest.write_bytes(body)
    responses.head(sources.trip_url(JAN), headers={"Content-Length": str(len(body))})
    extract.extract_trips(config, session, JAN)
    assert [call.request.method for call in responses.calls] == ["HEAD"]


@responses.activate
def test_extract_trips_redownloads_when_size_differs(config, session, tmp_path):
    body = parquet_bytes(tmp_path)
    dest = sources.local_path(config.data_dir, sources.TRIPS, sources.trip_filename(JAN))
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"stale")
    responses.head(sources.trip_url(JAN), headers={"Content-Length": str(len(body))})
    responses.get(sources.trip_url(JAN), body=body)
    extract.extract_trips(config, session, JAN)
    assert dest.read_bytes() == body


@responses.activate
def test_extract_weather_requests_the_month_and_validates(config, session):
    responses.get(sources.WEATHER_ARCHIVE_URL, json=weather_payload())
    info = extract.extract_weather(config, session, JAN)
    query = responses.calls[0].request.params
    assert (query["start_date"], query["end_date"]) == ("2025-01-01", "2025-01-31")
    assert info.row_count == 1
    assert json.loads(info.path.read_text())["daily"]["time"][0] == "2025-01-01"


@responses.activate
def test_extract_weather_rejects_incomplete_month(config, session):
    responses.get(sources.WEATHER_ARCHIVE_URL, json=weather_payload(days=10))
    with pytest.raises(sources.SchemaError):
        extract.extract_weather(config, session, JAN)
