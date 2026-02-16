"""Tests for tasks/fetch_ticker_prices.py.

Covers: PriceBar, _to_float, _to_int, _iter_stooq_daily_csv_rows,
        _stooq_symbol_for_ticker, _fetch_stooq_bar_for_date,
        _get_last_price_date.
"""
from __future__ import annotations

import types
from typing import Any, Dict, List, Optional

import requests

from scripts.sec_scraper.tasks.fetch_ticker_prices import (
    PriceBar,
    _fetch_stooq_bar_for_date,
    _get_last_price_date,
    _iter_stooq_daily_csv_rows,
    _stooq_symbol_for_ticker,
    _to_float,
    _to_int,
)


# ---------------------------------------------------------------------------
# PriceBar
# ---------------------------------------------------------------------------

def test_price_bar_creation():
    bar = PriceBar(
        ticker="AAPL",
        price_date="2025-06-15",
        open=150.0,
        high=155.0,
        low=149.0,
        close=154.0,
        volume=1000000,
        currency="USD",
        source="yahoo",
    )
    assert bar.ticker == "AAPL"
    assert bar.close == 154.0
    assert bar.source == "yahoo"


def test_price_bar_with_none_values():
    bar = PriceBar(
        ticker="TEST",
        price_date="2025-01-01",
        open=None,
        high=None,
        low=None,
        close=None,
        volume=None,
        currency=None,
        source="test",
    )
    assert bar.open is None
    assert bar.volume is None


# ---------------------------------------------------------------------------
# _to_float
# ---------------------------------------------------------------------------

def test_to_float_valid():
    assert _to_float("123.45") == 123.45
    assert _to_float("100") == 100.0
    assert _to_float(" 42.5 ") == 42.5


def test_to_float_na():
    assert _to_float("N/A") is None


def test_to_float_empty():
    assert _to_float("") is None
    assert _to_float("  ") is None


def test_to_float_invalid():
    assert _to_float("abc") is None


# ---------------------------------------------------------------------------
# _to_int
# ---------------------------------------------------------------------------

def test_to_int_valid():
    assert _to_int("100") == 100
    assert _to_int(" 42 ") == 42


def test_to_int_from_float_string():
    assert _to_int("100.7") == 100


def test_to_int_na():
    assert _to_int("N/A") is None


def test_to_int_empty():
    assert _to_int("") is None


def test_to_int_invalid():
    assert _to_int("xyz") is None


# ---------------------------------------------------------------------------
# _iter_stooq_daily_csv_rows
# ---------------------------------------------------------------------------

def test_iter_stooq_daily_csv_rows_parses_csv():
    csv_text = (
        "Date,Open,High,Low,Close,Volume\n"
        "2025-06-13,150.0,155.0,149.0,154.0,1000000\n"
        "2025-06-14,154.0,156.0,153.0,155.5,800000\n"
    )
    rows = list(_iter_stooq_daily_csv_rows(csv_text))
    assert len(rows) == 2
    assert rows[0]["Date"] == "2025-06-13"
    assert rows[0]["Close"] == "154.0"
    assert rows[1]["Volume"] == "800000"


def test_iter_stooq_daily_csv_rows_empty():
    rows = list(_iter_stooq_daily_csv_rows(""))
    assert rows == []


def test_iter_stooq_daily_csv_rows_header_only():
    csv_text = "Date,Open,High,Low,Close,Volume\n"
    rows = list(_iter_stooq_daily_csv_rows(csv_text))
    assert rows == []


# ---------------------------------------------------------------------------
# _stooq_symbol_for_ticker
# ---------------------------------------------------------------------------

def test_stooq_symbol_for_ticker():
    assert _stooq_symbol_for_ticker("AAPL") == "aapl.us"
    assert _stooq_symbol_for_ticker("msft") == "msft.us"
    assert _stooq_symbol_for_ticker("BRK.B") == "brk.b.us"


# ---------------------------------------------------------------------------
# _fetch_stooq_bar_for_date
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _StooqSession(requests.Session):
    def __init__(self, response: _FakeResponse):
        super().__init__()
        self._response = response

    def get(self, url: str, timeout: int = 30, **kwargs):  # type: ignore[override]
        return self._response


def test_fetch_stooq_bar_for_date_parses_csv():
    csv_text = (
        "Date,Open,High,Low,Close,Volume\n"
        "2025-06-13,150.0,155.0,149.0,154.0,1000000\n"
        "2025-06-14,154.0,156.0,153.0,155.5,800000\n"
    )
    session = _StooqSession(_FakeResponse(200, csv_text))

    bar = _fetch_stooq_bar_for_date(
        session, ticker="AAPL", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is not None
    assert bar.ticker == "AAPL"
    assert bar.price_date == "2025-06-14"
    assert bar.close == 155.5
    assert bar.volume == 800000
    assert bar.source == "stooq"


def test_fetch_stooq_bar_for_date_picks_latest_before_date():
    csv_text = (
        "Date,Open,High,Low,Close,Volume\n"
        "2025-06-10,100.0,110.0,99.0,105.0,500000\n"
        "2025-06-13,105.0,115.0,104.0,112.0,600000\n"
        "2025-06-16,112.0,120.0,111.0,118.0,700000\n"
    )
    session = _StooqSession(_FakeResponse(200, csv_text))

    # Request price for 2025-06-14 (weekend), should get 2025-06-13
    bar = _fetch_stooq_bar_for_date(
        session, ticker="TEST", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is not None
    assert bar.price_date == "2025-06-13"
    assert bar.close == 112.0


def test_fetch_stooq_bar_for_date_returns_none_on_404():
    session = _StooqSession(_FakeResponse(404))
    bar = _fetch_stooq_bar_for_date(
        session, ticker="BAD", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is None


def test_fetch_stooq_bar_for_date_returns_none_on_no_data():
    session = _StooqSession(_FakeResponse(200, "No data"))
    bar = _fetch_stooq_bar_for_date(
        session, ticker="UNKNOWN", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is None


def test_fetch_stooq_bar_for_date_returns_none_on_empty():
    session = _StooqSession(_FakeResponse(200, ""))
    bar = _fetch_stooq_bar_for_date(
        session, ticker="EMPTY", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is None


def test_fetch_stooq_bar_for_date_returns_none_when_all_dates_after():
    csv_text = (
        "Date,Open,High,Low,Close,Volume\n"
        "2025-07-01,100.0,110.0,99.0,105.0,500000\n"
    )
    session = _StooqSession(_FakeResponse(200, csv_text))

    bar = _fetch_stooq_bar_for_date(
        session, ticker="FUTURE", price_date="2025-06-14", timeout_s=5, rps=0,
    )
    assert bar is None


# ---------------------------------------------------------------------------
# _get_last_price_date (uses a fake cursor/connection)
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, result: Any = None):
        self._result = result

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self._result

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor_result: Any = None):
        self._cursor_result = cursor_result

    def cursor(self):
        return _FakeCursor(self._cursor_result)


def test_get_last_price_date_returns_date():
    conn = _FakeConn(cursor_result=("2025-12-01",))
    result = _get_last_price_date(conn, "sec_raw")
    assert result == "2025-12-01"


def test_get_last_price_date_returns_none_on_empty():
    conn = _FakeConn(cursor_result=(None,))
    result = _get_last_price_date(conn, "sec_raw")
    assert result is None
