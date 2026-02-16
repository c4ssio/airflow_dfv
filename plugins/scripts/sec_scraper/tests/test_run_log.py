"""Tests for postgres/run_log.py.

Uses fake cursor/connection objects to test without a real database.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from scripts.sec_scraper.postgres.run_log import (
    get_last_successful_run_date,
    record_completed_run,
)


class _FakeCursor:
    """Minimal cursor that records executed SQL and returns preset results."""

    def __init__(self, fetchone_result: Any = None):
        self._fetchone_result = fetchone_result
        self.executed: List[Tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Any:
        return self._fetchone_result

    def close(self) -> None:
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


# ---------------------------------------------------------------------------
# record_completed_run
# ---------------------------------------------------------------------------

def test_record_completed_run_inserts_and_commits():
    cur = _FakeCursor(fetchone_result=(42,))
    conn = _FakeConn(cur)

    run_id = record_completed_run(conn, "sec_raw", "2026-02-16", {"key": "val"})

    assert run_id == 42
    assert conn.committed
    assert len(cur.executed) == 1
    sql, params = cur.executed[0]
    assert "INSERT INTO sec_raw.pipeline_run_log" in sql
    assert params[0] == "2026-02-16"
    # Second param is the JSON summary
    assert json.loads(params[1]) == {"key": "val"}


def test_record_completed_run_none_summary():
    cur = _FakeCursor(fetchone_result=(1,))
    conn = _FakeConn(cur)

    run_id = record_completed_run(conn, "sec_raw", "2026-01-01")

    assert run_id == 1
    _, params = cur.executed[0]
    assert params[1] is None


# ---------------------------------------------------------------------------
# get_last_successful_run_date
# ---------------------------------------------------------------------------

def test_get_last_successful_run_date_returns_date():
    cur = _FakeCursor(fetchone_result=("2026-02-15",))
    conn = _FakeConn(cur)

    result = get_last_successful_run_date(conn, "sec_raw")
    assert result == "2026-02-15"


def test_get_last_successful_run_date_returns_none():
    cur = _FakeCursor(fetchone_result=(None,))
    conn = _FakeConn(cur)

    result = get_last_successful_run_date(conn, "sec_raw")
    assert result is None
