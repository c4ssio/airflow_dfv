"""Tests for tasks/validate_postgres_ingestion.py.

Covers: validate_postgres_ingestion with mocked DB connections.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.tasks.validate_postgres_ingestion import (
    validate_postgres_ingestion,
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


class _FakeCursor:
    """Cursor that returns pre-programmed results for each successive execute()."""

    def __init__(self, results: List[Any]):
        self._results = list(results)
        self._idx = 0

    def execute(self, sql: str, params=None):
        pass  # no-op

    def fetchone(self) -> Tuple:
        result = self._results[self._idx]
        self._idx += 1
        return (result,)

    def fetchall(self) -> List[Tuple]:
        result = self._results[self._idx]
        self._idx += 1
        return result

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — skip when no data
# ---------------------------------------------------------------------------

def test_validate_skips_when_no_ciks_ingested():
    cfg = _make_settings()
    result = validate_postgres_ingestion(cfg, {"total_ciks": 0})

    assert result["skipped"] is True
    assert result["validations_passed"] == 0
    assert result["validations_failed"] == 0


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — all checks pass
# ---------------------------------------------------------------------------

def test_validate_all_checks_pass():
    cfg = _make_settings()

    # Fake cursor results in order the queries execute:
    # 1. submissions count
    # 2. null CIK count
    # 3. array alignment (mismatched rows)
    # 4. facts with fiscal_year
    # 5. total facts
    # 6. duplicate CIKs
    fake_cursor = _FakeCursor([
        5,     # validation 1: 5 rows in submissions
        0,     # validation 2: 0 null CIKs
        [],    # validation 3: no mismatched rows
        100,   # validation 4: 100 facts with fy
        120,   # validation 4: 120 total facts
        [],    # validation 5: no duplicate CIKs
    ])
    fake_conn = _FakeConn(fake_cursor)

    with patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_config",
        return_value={"schema": "sec_raw"},
    ), patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_connection",
        return_value=fake_conn,
    ):
        result = validate_postgres_ingestion(cfg, {"total_ciks": 5, "processed": 5})

    assert result["validations_passed"] == 5
    assert result["validations_failed"] == 0
    assert len(result["details"]) == 5


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — critical failure (no rows)
# ---------------------------------------------------------------------------

def test_validate_critical_failure_no_rows():
    cfg = _make_settings()

    fake_cursor = _FakeCursor([
        0,     # validation 1: 0 rows -> critical failure
        0,     # validation 2: 0 null CIKs (pass)
        [],    # validation 3: no mismatched rows (pass)
        0,     # validation 4: 0 facts with fy
        0,     # validation 4: 0 total facts
        [],    # validation 5: no duplicates (pass)
    ])
    fake_conn = _FakeConn(fake_cursor)

    with patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_config",
        return_value={"schema": "sec_raw"},
    ), patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_connection",
        return_value=fake_conn,
    ):
        with pytest.raises(RuntimeError, match="Critical validation failures"):
            validate_postgres_ingestion(cfg, {"total_ciks": 5, "processed": 5})


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — null CIKs found
# ---------------------------------------------------------------------------

def test_validate_critical_failure_null_ciks():
    cfg = _make_settings()

    fake_cursor = _FakeCursor([
        10,    # validation 1: 10 rows (pass)
        3,     # validation 2: 3 null CIKs -> critical failure
        [],    # validation 3: no mismatched rows
        50,    # validation 4: facts with fy
        60,    # validation 4: total facts
        [],    # validation 5: no duplicates
    ])
    fake_conn = _FakeConn(fake_cursor)

    with patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_config",
        return_value={"schema": "sec_raw"},
    ), patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_connection",
        return_value=fake_conn,
    ):
        with pytest.raises(RuntimeError, match="Critical validation failures"):
            validate_postgres_ingestion(cfg, {"total_ciks": 10, "processed": 10})


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — array alignment warning (non-critical)
# ---------------------------------------------------------------------------

def test_validate_array_alignment_warning():
    cfg = _make_settings()

    fake_cursor = _FakeCursor([
        5,                               # validation 1: 5 rows
        0,                               # validation 2: 0 null CIKs
        [("CIK001", 3, 2)],             # validation 3: 1 mismatched row (warning, not failure)
        40,                              # validation 4: facts with fy
        50,                              # validation 4: total facts
        [],                              # validation 5: no duplicates
    ])
    fake_conn = _FakeConn(fake_cursor)

    with patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_config",
        return_value={"schema": "sec_raw"},
    ), patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_connection",
        return_value=fake_conn,
    ):
        result = validate_postgres_ingestion(cfg, {"total_ciks": 5, "processed": 5})

    # Array alignment is a warning, not a critical failure
    assert result["validations_failed"] == 0
    alignment_detail = [d for d in result["details"] if d["name"] == "array_alignment"][0]
    assert alignment_detail["status"] == "warning"
    assert alignment_detail["mismatched_ciks"] == ["CIK001"]


# ---------------------------------------------------------------------------
# validate_postgres_ingestion — duplicate CIKs
# ---------------------------------------------------------------------------

def test_validate_duplicate_ciks_failure():
    cfg = _make_settings()

    fake_cursor = _FakeCursor([
        10,                              # validation 1: 10 rows
        0,                               # validation 2: 0 null CIKs
        [],                              # validation 3: no mismatched
        80,                              # validation 4: facts with fy
        100,                             # validation 4: total facts
        [("CIK001", 2), ("CIK002", 3)], # validation 5: duplicates found
    ])
    fake_conn = _FakeConn(fake_cursor)

    with patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_config",
        return_value={"schema": "sec_raw"},
    ), patch(
        "scripts.sec_scraper.tasks.validate_postgres_ingestion.get_postgres_connection",
        return_value=fake_conn,
    ):
        # Duplicate CIKs are a non-critical failure (validations_failed increments
        # but no RuntimeError is raised because no critical_failure flag is set for PK dupes)
        result = validate_postgres_ingestion(cfg, {"total_ciks": 10, "processed": 10})

    assert result["validations_failed"] == 1
    pk_detail = [d for d in result["details"] if d["name"] == "primary_key_uniqueness"][0]
    assert pk_detail["status"] == "failed"
    assert "CIK001" in pk_detail["duplicate_ciks"]
