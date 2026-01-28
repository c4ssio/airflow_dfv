from __future__ import annotations

from typing import Any, Dict

import types

import requests

from plugins.scripts.sec_scraper.common import Settings
from plugins.scripts.sec_scraper.tasks.fetch_company_ciks import fetch_company_ciks


class DummySession(requests.Session):
    """Session that never actually hits the network."""

    def __init__(self, json_payload: Dict[str, Any]) -> None:
        super().__init__()
        self._json_payload = json_payload

    def get(self, url, timeout=None, **kwargs):  # type: ignore[override]
        response = types.SimpleNamespace()
        response.status_code = 200
        # mimic the requests.Response.json() method
        response.json = lambda: self._json_payload  # type: ignore[assignment]
        response.content = b"{}"
        return response


def _make_settings(**overrides: Any) -> Settings:
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
    return Settings(**base)  # type: ignore[arg-type]


def test_fetch_company_ciks_basic_normalization():
    # Arrange: minimal but realistic SEC payload
    payload: Dict[str, Any] = {
        "0": {"cik_str": 1000, "ticker": "AAA", "title": "Alpha Corp"},
        "1": {"cik_str": 2000, "ticker": "BBB", "title": "Beta Corp"},
        # Missing ticker should be filtered out
        "2": {"cik_str": 3000, "ticker": "", "title": "No Ticker Inc"},
    }
    cfg = _make_settings()
    session = DummySession(payload)

    # Act
    rows = fetch_company_ciks(cfg, session, "https://example.com/company_tickers.json")

    # Assert
    assert len(rows) == 2
    assert rows[0]["cik"] == "1000"
    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["title"] == "Alpha Corp"
    assert rows[1]["cik"] == "2000"


def test_fetch_company_ciks_respects_start_cik_and_max_ciks():
    payload: Dict[str, Any] = {
        "0": {"cik_str": 1000, "ticker": "AAA", "title": "Alpha Corp"},
        "1": {"cik_str": 2000, "ticker": "BBB", "title": "Beta Corp"},
        "2": {"cik_str": 3000, "ticker": "CCC", "title": "Gamma Corp"},
    }
    cfg = _make_settings(start_cik="2000", max_ciks=1)
    session = DummySession(payload)

    rows = fetch_company_ciks(cfg, session, "https://example.com/company_tickers.json")

    # Only entries with cik >= 2000, then limited to 1 result.
    assert len(rows) == 1
    assert rows[0]["cik"] == "2000"

