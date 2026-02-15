"""Tests for common.py functions not covered by test_common_converters.py.

Covers: load_settings, make_session, rate_limit, get_memory_mb, get_json.
"""
from __future__ import annotations

import os
import types
from typing import Any, Dict
from unittest.mock import patch

import pytest
import requests

from scripts.sec_scraper.common import (
    SecScraperConfigError,
    Settings,
    get_json,
    get_memory_mb,
    load_settings,
    make_session,
    rate_limit,
)


# ---------------------------------------------------------------------------
# load_settings
# ---------------------------------------------------------------------------

def test_load_settings_requires_sec_user_agent(tmp_path):
    env = {"SEC_USER_AGENT": ""}
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(SecScraperConfigError, match="SEC_USER_AGENT"):
            load_settings(str(tmp_path / "dag.py"))


def test_load_settings_returns_defaults(tmp_path):
    config_file = tmp_path / "config" / "postgres.yaml"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text("host: localhost\n")

    env = {
        "SEC_USER_AGENT": "test (test@example.com)",
        "POSTGRES_CONFIG_PATH": str(config_file),
    }
    with patch.dict(os.environ, env, clear=True):
        s = load_settings(str(tmp_path / "dag.py"))

    assert s.user_agent == "test (test@example.com)"
    assert s.rps == 5.0
    assert s.timeout_s == 30
    assert s.max_ciks == 250
    assert s.start_cik == ""
    assert s.s3_bucket == ""
    assert s.s3_prefix == "sec_raw"
    assert s.local_dir == "/tmp/sec_raw"
    assert s.ingest_test_cik == ""
    assert s.ingest_max_ciks == 0


def test_load_settings_reads_custom_env(tmp_path):
    config_file = tmp_path / "pg.yaml"
    config_file.write_text("host: db\n")

    env = {
        "SEC_USER_AGENT": "agent",
        "SEC_REQUESTS_PER_SECOND": "10",
        "SEC_TIMEOUT_SECONDS": "60",
        "SEC_MAX_CIKS_PER_RUN": "50",
        "SEC_START_CIK": "5000",
        "SEC_S3_BUCKET": "my-bucket",
        "SEC_S3_PREFIX": "raw/sec",
        "SEC_LOCAL_DIR": "/data/sec",
        "SEC_INGEST_TEST_CIK": "0000000001",
        "SEC_INGEST_MAX_CIKS": "10",
        "POSTGRES_CONFIG_PATH": str(config_file),
    }
    with patch.dict(os.environ, env, clear=True):
        s = load_settings(str(tmp_path / "dag.py"))

    assert s.rps == 10.0
    assert s.timeout_s == 60
    assert s.max_ciks == 50
    assert s.start_cik == "5000"
    assert s.s3_bucket == "my-bucket"
    assert s.s3_prefix == "raw/sec"
    assert s.local_dir == "/data/sec"
    assert s.ingest_test_cik == "0000000001"
    assert s.ingest_max_ciks == 10


# ---------------------------------------------------------------------------
# make_session
# ---------------------------------------------------------------------------

def test_make_session_sets_user_agent_header():
    s = make_session("test-agent (test@example.com)")
    assert s.headers["User-Agent"] == "test-agent (test@example.com)"
    assert s.headers["Accept"] == "application/json"


# ---------------------------------------------------------------------------
# rate_limit
# ---------------------------------------------------------------------------

def test_rate_limit_does_not_error_for_positive_rps():
    # Just verify it runs without raising
    rate_limit(10.0)


def test_rate_limit_noop_for_zero_rps():
    rate_limit(0)


def test_rate_limit_noop_for_negative_rps():
    rate_limit(-1)


# ---------------------------------------------------------------------------
# get_memory_mb
# ---------------------------------------------------------------------------

def test_get_memory_mb_returns_non_negative():
    mem = get_memory_mb()
    assert isinstance(mem, float)
    assert mem >= 0.0


# ---------------------------------------------------------------------------
# get_json
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, status_code: int, payload: Any = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.content = b"{}"

    def json(self) -> Any:
        return self._payload


class _DummySessionForGetJson(requests.Session):
    """Session that returns canned responses without hitting the network."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        super().__init__()
        self._responses = list(responses)
        self._call_count = 0

    def get(self, url: str, timeout: int = 30, **kwargs):  # type: ignore[override]
        resp = self._responses[min(self._call_count, len(self._responses) - 1)]
        self._call_count += 1
        return resp


def test_get_json_returns_payload_on_200():
    payload = {"key": "value"}
    session = _DummySessionForGetJson([_FakeResponse(200, payload)])
    result = get_json(session, "https://example.com/data.json", timeout_s=5, rps=0)
    assert result == payload


def test_get_json_retries_on_429_then_succeeds():
    payload = {"ok": True}
    session = _DummySessionForGetJson([
        _FakeResponse(429),
        _FakeResponse(200, payload),
    ])
    result = get_json(session, "https://example.com/data.json", timeout_s=5, rps=0, max_attempts=3)
    assert result == payload


def test_get_json_retries_on_503_then_succeeds():
    payload = {"ok": True}
    session = _DummySessionForGetJson([
        _FakeResponse(503),
        _FakeResponse(200, payload),
    ])
    result = get_json(session, "https://example.com/data.json", timeout_s=5, rps=0, max_attempts=3)
    assert result == payload


def test_get_json_raises_on_non_retryable_status():
    session = _DummySessionForGetJson([_FakeResponse(404, text="Not Found")])
    with pytest.raises(RuntimeError, match="status=404"):
        get_json(session, "https://example.com/missing.json", timeout_s=5, rps=0)


def test_get_json_raises_after_max_attempts_exhausted():
    session = _DummySessionForGetJson([_FakeResponse(429)] * 3)
    with pytest.raises(RuntimeError, match="after 2 attempts"):
        get_json(session, "https://example.com/data.json", timeout_s=5, rps=0, max_attempts=2)
