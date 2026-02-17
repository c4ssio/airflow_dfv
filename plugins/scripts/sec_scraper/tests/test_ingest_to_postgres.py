"""Tests for tasks/ingest_to_postgres.py.

Covers: _discover_cik_dirs, _build_ndjson_for_cik, _write_ndjson_file,
        _cik_from_dir_name, ingest_ciks.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Set
from unittest.mock import MagicMock

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.tasks.ingest_to_postgres import (
    _build_ndjson_for_cik,
    _cik_from_dir_name,
    _discover_cik_dirs,
    _write_ndjson_file,
    ingest_ciks,
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
# _cik_from_dir_name
# ---------------------------------------------------------------------------

def test_cik_from_dir_name_pads_to_10():
    assert _cik_from_dir_name("cik=1234") == "0000001234"


def test_cik_from_dir_name_strips_leading_zeros():
    assert _cik_from_dir_name("cik=0000001234") == "0000001234"


def test_cik_from_dir_name_handles_zero():
    assert _cik_from_dir_name("cik=0") == "0000000000"


# ---------------------------------------------------------------------------
# _write_ndjson_file
# ---------------------------------------------------------------------------

def test_write_ndjson_file_creates_file(tmp_path):
    path = str(tmp_path / "sub" / "data.ndjson")
    rows = [{"cik": "1", "name": "Test"}, {"cik": "2", "name": "Test2"}]
    _write_ndjson_file(path, rows)

    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0])["cik"] == "1"
    assert json.loads(lines[1])["cik"] == "2"


def test_write_ndjson_file_empty_rows(tmp_path):
    path = str(tmp_path / "empty.ndjson")
    _write_ndjson_file(path, [])
    with open(path, "r", encoding="utf-8") as f:
        assert f.read().strip() == ""


# ---------------------------------------------------------------------------
# _discover_cik_dirs
# ---------------------------------------------------------------------------

def test_discover_cik_dirs_finds_cik_directories(tmp_path):
    (tmp_path / "cik=100").mkdir()
    (tmp_path / "cik=200").mkdir()
    (tmp_path / "other_dir").mkdir()
    (tmp_path / "somefile.txt").write_text("")

    cfg = _make_settings(local_dir=str(tmp_path))
    dirs = _discover_cik_dirs(cfg)
    assert sorted(dirs) == ["cik=100", "cik=200"]


def test_discover_cik_dirs_empty_when_no_base(tmp_path):
    cfg = _make_settings(local_dir=str(tmp_path / "nonexistent"))
    assert _discover_cik_dirs(cfg) == []


def test_discover_cik_dirs_filters_to_test_cik(tmp_path):
    (tmp_path / "cik=100").mkdir()
    (tmp_path / "cik=200").mkdir()

    cfg = _make_settings(local_dir=str(tmp_path), ingest_test_cik="200")
    dirs = _discover_cik_dirs(cfg)
    assert dirs == ["cik=200"]


def test_discover_cik_dirs_test_cik_not_found(tmp_path):
    (tmp_path / "cik=100").mkdir()

    cfg = _make_settings(local_dir=str(tmp_path), ingest_test_cik="999")
    assert _discover_cik_dirs(cfg) == []


def test_discover_cik_dirs_respects_max_ciks(tmp_path):
    for i in range(5):
        (tmp_path / f"cik={i}").mkdir()

    cfg = _make_settings(local_dir=str(tmp_path), ingest_max_ciks=2)
    dirs = _discover_cik_dirs(cfg)
    assert len(dirs) == 2


# ---------------------------------------------------------------------------
# _build_ndjson_for_cik
# ---------------------------------------------------------------------------

def test_build_ndjson_for_cik_with_submissions_only(tmp_path):
    cik_dir = tmp_path / "cik=1234"
    cik_dir.mkdir()
    submissions = {"name": "Test Corp", "tickers": ["T"], "exchanges": ["NYSE"]}
    (cik_dir / "submissions.json").write_text(json.dumps(submissions))

    result = _build_ndjson_for_cik(str(tmp_path), "cik=1234", "2025-01-01")
    sub_paths, meta_paths, facts_paths, ciks_sub, ciks_meta, ciks_facts = result

    assert len(sub_paths) == 1
    assert len(meta_paths) == 0
    assert len(facts_paths) == 0
    assert ciks_sub == ["1234"]
    assert os.path.exists(sub_paths[0])


def test_build_ndjson_for_cik_with_submissions_and_companyfacts(tmp_path):
    cik_dir = tmp_path / "cik=5678"
    cik_dir.mkdir()

    submissions = {"name": "Test Corp", "tickers": [], "exchanges": []}
    (cik_dir / "submissions.json").write_text(json.dumps(submissions))

    companyfacts = {
        "entityName": "Test Corp",
        "facts": {
            "us-gaap": {
                "Revenue": {
                    "label": "Revenue",
                    "description": "Total revenue",
                    "units": {
                        "USD": [{
                            "fy": 2024,
                            "val": 1000000,
                            "end": "2024-12-31",
                            "filed": "2025-02-01",
                            "accn": "0001-25-000001",
                            "fp": "FY",
                            "form": "10-K",
                        }]
                    },
                }
            }
        },
    }
    (cik_dir / "companyfacts.json").write_text(json.dumps(companyfacts))

    result = _build_ndjson_for_cik(str(tmp_path), "cik=5678", "2025-01-01")
    sub_paths, meta_paths, facts_paths, ciks_sub, ciks_meta, ciks_facts = result

    assert len(sub_paths) == 1
    assert len(meta_paths) == 1
    assert len(facts_paths) == 1
    assert ciks_sub == ["5678"]
    assert ciks_meta == ["5678"]
    assert ciks_facts == ["5678"]


def test_build_ndjson_for_cik_missing_both_files(tmp_path):
    cik_dir = tmp_path / "cik=0000"
    cik_dir.mkdir()
    # No JSON files inside

    result = _build_ndjson_for_cik(str(tmp_path), "cik=0000", "2025-01-01")
    sub_paths, meta_paths, facts_paths, ciks_sub, ciks_meta, ciks_facts = result

    assert sub_paths == []
    assert meta_paths == []
    assert facts_paths == []


# ---------------------------------------------------------------------------
# ingest_ciks
# ---------------------------------------------------------------------------

class _FakeLoadFns:
    """Fake DB loader that records calls instead of touching a real database."""

    def __init__(self, already_ingested: Set[str] | None = None):
        self.calls: List[Dict[str, Any]] = []
        self._already_ingested = already_ingested or set()
        self._conn = MagicMock()

    def get_connection(self, config, schema):
        return self._conn

    def load_ndjson_batch(self, conn, table_name, paths, cik_list, ingest_date, schema):
        self.calls.append({
            "table": table_name,
            "paths": paths,
            "cik_list": cik_list,
        })
        return len(paths)


def test_ingest_ciks_processes_cik_directories(tmp_path, monkeypatch):
    # Set up CIK directories with submissions.json
    for cik in ["100", "200"]:
        cik_dir = tmp_path / f"cik={cik}"
        cik_dir.mkdir()
        submissions = {"name": f"Corp {cik}", "tickers": [], "exchanges": []}
        (cik_dir / "submissions.json").write_text(json.dumps(submissions))

    cfg = _make_settings(local_dir=str(tmp_path))
    postgres_config = {"schema": "sec_raw"}
    loader = _FakeLoadFns()

    # Patch DB check functions to return empty sets (nothing ingested)
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_ciks_already_ingested",
        lambda conn, schema, date: set(),
    )
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_previously_ingested_from_set",
        lambda conn, schema, cik_set: set(),
    )

    result = ingest_ciks(cfg, postgres_config, loader, upsert_metric_metadata_fn=MagicMock())

    assert result["total_ciks"] == 2
    assert result["processed"] == 2
    assert result["errors"] == 0
    assert result["skipped"] == 0
    # submissions should have been loaded
    table_names = [c["table"] for c in loader.calls]
    assert "submissions" in table_names


def test_ingest_ciks_skips_already_ingested(tmp_path, monkeypatch):
    cik_dir = tmp_path / "cik=100"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text(json.dumps({"name": "Corp"}))

    cfg = _make_settings(local_dir=str(tmp_path))
    postgres_config = {"schema": "sec_raw"}
    loader = _FakeLoadFns()

    # CIK 100 -> zero-padded is 0000000100
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_ciks_already_ingested",
        lambda conn, schema, date: {"0000000100"},
    )
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_previously_ingested_from_set",
        lambda conn, schema, cik_set: set(),
    )

    result = ingest_ciks(cfg, postgres_config, loader, upsert_metric_metadata_fn=MagicMock())

    assert result["total_ciks"] == 1
    assert result["processed"] == 0
    assert result["skipped"] == 1
    assert loader.calls == []


def test_ingest_ciks_returns_zeros_when_no_dirs(tmp_path, monkeypatch):
    cfg = _make_settings(local_dir=str(tmp_path / "empty"))
    postgres_config = {"schema": "sec_raw"}
    loader = _FakeLoadFns()

    result = ingest_ciks(cfg, postgres_config, loader, upsert_metric_metadata_fn=MagicMock())

    assert result == {"total_ciks": 0, "processed": 0, "errors": 0, "skipped": 0}


def test_ingest_ciks_force_ingest_overrides_skip_logic(tmp_path, monkeypatch):
    """Test that force_ingest_ciks parameter allows re-ingestion of existing CIKs."""
    # Set up CIK directory with submissions.json
    cik_dir = tmp_path / "cik=100"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text(json.dumps({"name": "Corp"}))

    cfg = _make_settings(local_dir=str(tmp_path))
    postgres_config = {"schema": "sec_raw"}
    loader = _FakeLoadFns()

    # CIK 100 was previously ingested (any date)
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_ciks_already_ingested",
        lambda conn, schema, date: set(),  # Not ingested TODAY
    )
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_previously_ingested_from_set",
        lambda conn, schema, cik_set: {"0000000100"},  # Previously ingested
    )

    # Without force_ingest_ciks, CIK should be skipped
    result = ingest_ciks(cfg, postgres_config, loader, upsert_metric_metadata_fn=MagicMock())
    assert result["skipped"] == 1
    assert result["processed"] == 0

    # Reset loader
    loader.calls = []

    # With force_ingest_ciks, CIK should be processed
    result = ingest_ciks(
        cfg,
        postgres_config,
        loader,
        upsert_metric_metadata_fn=MagicMock(),
        force_ingest_ciks={"0000000100"},
    )
    assert result["processed"] == 1
    assert result["skipped"] == 0
    assert len(loader.calls) > 0


def test_ingest_ciks_force_ingest_overrides_same_day(tmp_path, monkeypatch):
    """Force-ingest CIKs are retried even if partially ingested today."""
    cik_dir = tmp_path / "cik=100"
    cik_dir.mkdir()
    (cik_dir / "submissions.json").write_text(json.dumps({"name": "Corp"}))

    cfg = _make_settings(local_dir=str(tmp_path))
    postgres_config = {"schema": "sec_raw"}
    loader = _FakeLoadFns()

    # CIK 100 was already ingested today (partial — e.g. submissions OK, facts failed)
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_ciks_already_ingested",
        lambda conn, schema, date: {"0000000100"},
    )
    monkeypatch.setattr(
        "scripts.sec_scraper.tasks.ingest_to_postgres.get_previously_ingested_from_set",
        lambda conn, schema, cik_set: {"0000000100"},
    )

    # With force_ingest_ciks, CIK should be re-processed despite same-day ingestion
    result = ingest_ciks(
        cfg,
        postgres_config,
        loader,
        upsert_metric_metadata_fn=MagicMock(),
        force_ingest_ciks={"0000000100"},
    )
    assert result["processed"] == 1
    assert result["skipped"] == 0
    assert len(loader.calls) > 0
