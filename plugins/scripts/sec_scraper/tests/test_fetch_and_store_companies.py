"""Tests for tasks/fetch_and_store_companies.py.

Covers: process_single_company, estimate_results_size_mb, fetch_and_store_companies.
"""
from __future__ import annotations

import json
import types
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import requests
import pytest

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.tasks.fetch_and_store_companies import (
    estimate_results_size_mb,
    fetch_and_store_companies,
    process_single_company,
)


def _make_settings(**overrides) -> Settings:
    base = dict(
        user_agent="test-agent",
        rps=0,  # disable rate limit in tests
        timeout_s=5,
        max_ciks=250,
        start_cik="",
        s3_bucket="",
        s3_prefix="sec_raw",
        local_dir="/tmp/test_sec_raw",
        postgres_config_path="/tmp/postgres.yaml",
        ingest_test_cik="",
        ingest_max_ciks=0,
    )
    base.update(overrides)
    return Settings(**base)


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = json.dumps(self._payload).encode()
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


class _DummySession(requests.Session):
    """Session that returns canned responses by URL substring."""

    def __init__(self, url_payloads: Dict[str, Any]) -> None:
        super().__init__()
        self._url_payloads = url_payloads

    def get(self, url: str, timeout: int = 30, **kwargs):  # type: ignore[override]
        for substr, payload in self._url_payloads.items():
            if substr in url:
                return _FakeResponse(200, payload)
        return _FakeResponse(200, {})


# ---------------------------------------------------------------------------
# estimate_results_size_mb
# ---------------------------------------------------------------------------

def test_estimate_results_size_mb_empty():
    assert estimate_results_size_mb([]) == 0.0


def test_estimate_results_size_mb_non_empty():
    results = [{"cik": "1234", "stored": "local"}]
    size = estimate_results_size_mb(results)
    assert size > 0.0
    assert size < 1.0  # a single small dict should be well under 1 MB


# ---------------------------------------------------------------------------
# process_single_company
# ---------------------------------------------------------------------------

def test_process_single_company_raises_without_required_fns():
    cfg = _make_settings()
    session = _DummySession({})
    company = {"cik": "1", "ticker": "AAA"}
    with pytest.raises(RuntimeError, match="find_existing_data_fn"):
        process_single_company(cfg, session, company, sec_base="https://sec.gov")


def test_process_single_company_downloads_and_stores(tmp_path):
    local_dir = str(tmp_path)
    cfg = _make_settings(local_dir=local_dir)

    submissions_payload = {
        "name": "Test Corp",
        "filings": {"recent": {"filingDate": ["2025-06-01"]}},
    }
    facts_payload = {"entityName": "Test Corp", "facts": {}}

    session = _DummySession({
        "submissions/CIK": submissions_payload,
        "companyfacts/CIK": facts_payload,
    })

    written_files: Dict[str, bytes] = {}
    written_metadata: List[Dict] = []

    def fake_write_bytes(path: str, data: bytes) -> None:
        written_files[path] = data

    def fake_write_metadata(cik_dir, filing_date, ingest_date) -> None:
        written_metadata.append({
            "cik_dir": cik_dir,
            "filing_date": filing_date,
            "ingest_date": ingest_date,
        })

    result = process_single_company(
        cfg,
        session,
        {"cik": "1234", "ticker": "TEST"},
        sec_base="https://data.sec.gov",
        find_existing_data_fn=lambda cfg, cik: None,
        read_metadata_fn=lambda path: None,
        write_bytes_fn=fake_write_bytes,
        write_metadata_fn=fake_write_metadata,
    )

    assert result["cik"] == "1234"
    assert result["ticker"] == "TEST"
    assert result["stored"] == "local"
    assert result["facts_downloaded"] is True
    assert len(written_files) == 2  # submissions + companyfacts
    assert len(written_metadata) == 1


def test_process_single_company_skips_facts_when_no_new_filings(tmp_path):
    local_dir = str(tmp_path)
    cfg = _make_settings(local_dir=local_dir)

    submissions_payload = {
        "name": "Old Corp",
        "filings": {"recent": {"filingDate": ["2025-01-01"]}},
    }

    session = _DummySession({
        "submissions/CIK": submissions_payload,
    })

    written_files: Dict[str, bytes] = {}

    def fake_find_existing(cfg, cik):
        return {"metadata": f"{local_dir}/cik={cik}/metadata.json"}

    def fake_read_metadata(path):
        return {"latest_filing_date": "2025-06-01"}  # newer than new_filing_date

    result = process_single_company(
        cfg,
        session,
        {"cik": "5678", "ticker": "OLD"},
        sec_base="https://data.sec.gov",
        find_existing_data_fn=fake_find_existing,
        read_metadata_fn=fake_read_metadata,
        write_bytes_fn=lambda path, data: written_files.update({path: data}),
        write_metadata_fn=lambda *a: None,
    )

    assert result["facts_downloaded"] is False
    # Only submissions written, not companyfacts
    assert len(written_files) == 1
    assert any("submissions.json" in p for p in written_files)


def test_process_single_company_raises_on_s3_bucket():
    cfg = _make_settings(s3_bucket="my-bucket")
    session = _DummySession({"submissions/CIK": {"filings": {"recent": {"filingDate": []}}}})

    with pytest.raises(RuntimeError, match="S3"):
        process_single_company(
            cfg,
            session,
            {"cik": "1", "ticker": "X"},
            sec_base="https://data.sec.gov",
            find_existing_data_fn=lambda cfg, cik: None,
            read_metadata_fn=lambda path: None,
            write_bytes_fn=lambda path, data: None,
            write_metadata_fn=lambda *a: None,
        )


# ---------------------------------------------------------------------------
# fetch_and_store_companies
# ---------------------------------------------------------------------------

def test_fetch_and_store_companies_returns_summary(tmp_path, monkeypatch):
    local_dir = str(tmp_path)
    cfg = _make_settings(local_dir=local_dir)

    submissions_payload = {
        "name": "Corp",
        "filings": {"recent": {"filingDate": ["2025-06-01"]}},
    }
    facts_payload = {"entityName": "Corp", "facts": {}}

    session = _DummySession({
        "submissions/CIK": submissions_payload,
        "companyfacts/CIK": facts_payload,
    })

    companies = [
        {"cik": "1000", "ticker": "AAA"},
        {"cik": "2000", "ticker": "BBB"},
    ]

    result = fetch_and_store_companies(cfg, session, companies, "https://data.sec.gov")

    assert result["total_companies"] == 2
    assert result["facts_downloaded"] + result["facts_skipped"] == 2
    assert result["results_file"] is not None


def test_fetch_and_store_companies_handles_empty_list(tmp_path):
    cfg = _make_settings(local_dir=str(tmp_path))
    session = _DummySession({})

    result = fetch_and_store_companies(cfg, session, [], "https://data.sec.gov")

    assert result["total_companies"] == 0
    assert result["facts_downloaded"] == 0


def test_fetch_and_store_companies_tracks_ciks_needing_ingest(tmp_path, monkeypatch):
    """Test that CIKs with new filings or first-time loads are tracked for ingestion."""
    local_dir = str(tmp_path)
    cfg = _make_settings(local_dir=local_dir)

    submissions_payload = {
        "name": "Corp",
        "filings": {"recent": {"filingDate": ["2025-06-01"]}},
    }
    facts_payload = {"entityName": "Corp", "facts": {}}

    session = _DummySession({
        "submissions/CIK": submissions_payload,
        "companyfacts/CIK": facts_payload,
    })

    companies = [
        {"cik": "1000", "ticker": "AAA"},  # First-time CIK
        {"cik": "2000", "ticker": "BBB"},  # First-time CIK
    ]

    result = fetch_and_store_companies(cfg, session, companies, "https://data.sec.gov")

    assert result["total_companies"] == 2
    assert "ciks_needing_ingest" in result
    # Both are first-time loads, so both should be in the list
    assert len(result["ciks_needing_ingest"]) == 2
    assert "0000001000" in result["ciks_needing_ingest"]
    assert "0000002000" in result["ciks_needing_ingest"]
