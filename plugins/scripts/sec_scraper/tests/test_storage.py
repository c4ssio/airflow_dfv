"""Tests for storage.py.

Covers: s3_key, write_bytes, get_most_recent_filing_date, read_metadata,
        write_metadata, find_existing_data.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.storage import (
    find_existing_data,
    get_most_recent_filing_date,
    read_metadata,
    s3_key,
    write_bytes,
    write_metadata,
)


def _make_settings(**overrides) -> Settings:
    base = dict(
        user_agent="test-agent",
        rps=5.0,
        timeout_s=30,
        max_ciks=250,
        start_cik="",
        s3_bucket="",
        s3_prefix="sec_raw",
        local_dir="/tmp/sec_raw",
        postgres_config_path="/tmp/postgres.yaml",
        ingest_test_cik="",
        ingest_max_ciks=0,
    )
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# s3_key
# ---------------------------------------------------------------------------

def test_s3_key_builds_correct_path():
    assert s3_key("raw", "1234", "submissions") == "raw/cik=1234/submissions.json"


def test_s3_key_with_nested_prefix():
    assert s3_key("data/sec", "99", "companyfacts") == "data/sec/cik=99/companyfacts.json"


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------

def test_write_bytes_creates_directories_and_writes(tmp_path):
    target = str(tmp_path / "a" / "b" / "file.json")
    data = b'{"hello": "world"}'
    write_bytes(target, data)
    with open(target, "rb") as f:
        assert f.read() == data


def test_write_bytes_overwrites_existing(tmp_path):
    target = str(tmp_path / "file.json")
    write_bytes(target, b"first")
    write_bytes(target, b"second")
    with open(target, "rb") as f:
        assert f.read() == b"second"


# ---------------------------------------------------------------------------
# get_most_recent_filing_date
# ---------------------------------------------------------------------------

def test_get_most_recent_filing_date_extracts_max():
    data: Dict[str, Any] = {
        "filings": {
            "recent": {
                "filingDate": ["2025-01-01", "2025-06-15", "2025-03-10"],
            }
        }
    }
    assert get_most_recent_filing_date(data) == "2025-06-15"


def test_get_most_recent_filing_date_returns_none_no_filings():
    assert get_most_recent_filing_date({}) is None


def test_get_most_recent_filing_date_returns_none_empty_dates():
    data: Dict[str, Any] = {"filings": {"recent": {"filingDate": []}}}
    assert get_most_recent_filing_date(data) is None


def test_get_most_recent_filing_date_returns_none_no_recent():
    data: Dict[str, Any] = {"filings": {}}
    assert get_most_recent_filing_date(data) is None


# ---------------------------------------------------------------------------
# read_metadata / write_metadata
# ---------------------------------------------------------------------------

def test_write_metadata_creates_file(tmp_path):
    cik_dir = str(tmp_path / "cik=1234")
    write_metadata(cik_dir, "2025-06-15", "2025-06-16")

    meta = read_metadata(cik_dir)
    assert meta is not None
    assert meta["latest_filing_date"] == "2025-06-15"
    assert meta["last_updated"] == "2025-06-16"
    assert "updated_at" in meta


def test_read_metadata_returns_none_when_missing(tmp_path):
    assert read_metadata(str(tmp_path / "nonexistent")) is None


def test_read_metadata_returns_none_on_corrupt_file(tmp_path):
    cik_dir = tmp_path / "cik=bad"
    cik_dir.mkdir()
    (cik_dir / "metadata.json").write_text("NOT VALID JSON {{{{")
    assert read_metadata(str(cik_dir)) is None


def test_write_metadata_with_none_filing_date(tmp_path):
    cik_dir = str(tmp_path / "cik=0")
    write_metadata(cik_dir, None, "2025-01-01")
    meta = read_metadata(cik_dir)
    assert meta is not None
    assert meta["latest_filing_date"] is None


# ---------------------------------------------------------------------------
# find_existing_data
# ---------------------------------------------------------------------------

def test_find_existing_data_returns_paths_when_present(tmp_path):
    cik_dir = tmp_path / "cik=999"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text("{}")
    (cik_dir / "companyfacts.json").write_text("{}")
    (cik_dir / "metadata.json").write_text("{}")

    cfg = _make_settings(local_dir=str(tmp_path))
    result = find_existing_data(cfg, "999")
    assert result is not None
    assert "submissions" in result
    assert "companyfacts" in result
    assert "metadata" in result


def test_find_existing_data_returns_none_when_no_dir(tmp_path):
    cfg = _make_settings(local_dir=str(tmp_path))
    assert find_existing_data(cfg, "999") is None


def test_find_existing_data_returns_none_without_submissions(tmp_path):
    cik_dir = tmp_path / "cik=999"
    cik_dir.mkdir()
    (cik_dir / "metadata.json").write_text("{}")
    # No submissions.json

    cfg = _make_settings(local_dir=str(tmp_path))
    assert find_existing_data(cfg, "999") is None


def test_find_existing_data_returns_none_without_metadata(tmp_path):
    cik_dir = tmp_path / "cik=999"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text("{}")
    # No metadata.json

    cfg = _make_settings(local_dir=str(tmp_path))
    assert find_existing_data(cfg, "999") is None


def test_find_existing_data_omits_companyfacts_when_absent(tmp_path):
    cik_dir = tmp_path / "cik=999"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text("{}")
    (cik_dir / "metadata.json").write_text("{}")

    cfg = _make_settings(local_dir=str(tmp_path))
    result = find_existing_data(cfg, "999")
    assert result is not None
    assert "companyfacts" not in result


def test_find_existing_data_returns_none_for_s3(tmp_path):
    cfg = _make_settings(local_dir=str(tmp_path), s3_bucket="my-bucket")
    assert find_existing_data(cfg, "999") is None
