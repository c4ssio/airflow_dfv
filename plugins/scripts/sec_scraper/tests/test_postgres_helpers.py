"""Tests for postgres/helpers.py.

Covers: get_postgres_config (YAML + env-var fallback), write_ndjson_file.
(Connection-dependent functions like get_postgres_connection, load_ndjson_batch_to_postgres,
upsert_metric_metadata require a live DB and are covered by integration tests.)
"""
from __future__ import annotations

import json
import os

import pytest

from scripts.sec_scraper.postgres.helpers import (
    get_postgres_config,
    write_ndjson_file,
)


# ---------------------------------------------------------------------------
# get_postgres_config
# ---------------------------------------------------------------------------

def test_get_postgres_config_loads_yaml(tmp_path):
    config_file = tmp_path / "postgres.yaml"
    config_file.write_text(
        "host: localhost\n"
        "port: 5432\n"
        "database: testdb\n"
        "user: testuser\n"
        "password: testpass\n"
        "schema: sec_raw\n"
    )
    config = get_postgres_config(str(config_file))
    assert config["host"] == "localhost"
    assert config["port"] == 5432
    assert config["database"] == "testdb"
    assert config["user"] == "testuser"
    assert config["password"] == "testpass"
    assert config["schema"] == "sec_raw"


def test_get_postgres_config_raises_on_missing_file(tmp_path):
    with pytest.raises(RuntimeError, match="not found"):
        get_postgres_config(str(tmp_path / "nonexistent.yaml"))


def test_get_postgres_config_raises_on_missing_keys(tmp_path):
    config_file = tmp_path / "bad.yaml"
    config_file.write_text("host: localhost\nport: 5432\n")
    with pytest.raises(RuntimeError, match="Missing required"):
        get_postgres_config(str(config_file))


def test_get_postgres_config_env_var_fallback(tmp_path, monkeypatch):
    """When POSTGRES_HOST is set, env vars are used instead of YAML."""
    monkeypatch.setenv("POSTGRES_HOST", "rds.example.com")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "mydb")
    monkeypatch.setenv("POSTGRES_USER", "admin")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")

    # YAML file doesn't even need to exist
    config = get_postgres_config(str(tmp_path / "nonexistent.yaml"))
    assert config["host"] == "rds.example.com"
    assert config["port"] == 5433
    assert config["database"] == "mydb"
    assert config["user"] == "admin"
    assert config["password"] == "secret"
    assert config["schema"] == "public"


def test_get_postgres_config_env_var_defaults(monkeypatch):
    """When only POSTGRES_HOST is set, other values use sensible defaults."""
    monkeypatch.setenv("POSTGRES_HOST", "db.local")
    # Clear other vars if set
    for k in ("POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_SCHEMA"):
        monkeypatch.delenv(k, raising=False)

    config = get_postgres_config("/nonexistent")
    assert config["host"] == "db.local"
    assert config["port"] == 5432
    assert config["database"] == "sec_data"
    assert config["user"] == "airflow"
    assert config["password"] == "airflow"
    assert config["schema"] == "sec_raw"


def test_get_postgres_config_yaml_when_no_env_host(tmp_path, monkeypatch):
    """When POSTGRES_HOST is empty, falls back to YAML."""
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    config_file = tmp_path / "postgres.yaml"
    config_file.write_text(
        "host: yaml-host\nport: 5432\ndatabase: testdb\nuser: u\npassword: p\n"
    )
    config = get_postgres_config(str(config_file))
    assert config["host"] == "yaml-host"


# ---------------------------------------------------------------------------
# write_ndjson_file
# ---------------------------------------------------------------------------

def test_write_ndjson_file_creates_file_with_rows(tmp_path):
    path = str(tmp_path / "output.ndjson")
    rows = [
        {"cik": "0000000001", "name": "Corp A"},
        {"cik": "0000000002", "name": "Corp B"},
    ]
    write_ndjson_file(path, rows)

    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert parsed["cik"] == "0000000001"
    assert parsed["name"] == "Corp A"


def test_write_ndjson_file_creates_parent_dirs(tmp_path):
    path = str(tmp_path / "deep" / "nested" / "output.ndjson")
    write_ndjson_file(path, [{"key": "val"}])
    assert (tmp_path / "deep" / "nested" / "output.ndjson").exists()


def test_write_ndjson_file_empty_rows(tmp_path):
    path = str(tmp_path / "empty.ndjson")
    write_ndjson_file(path, [])
    with open(path, "r", encoding="utf-8") as f:
        assert f.read().strip() == ""


def test_write_ndjson_file_handles_nested_json(tmp_path):
    path = str(tmp_path / "nested.ndjson")
    rows = [{"name": "Test", "addresses": {"city": "NYC", "state": "NY"}}]
    write_ndjson_file(path, rows)

    with open(path, "r", encoding="utf-8") as f:
        parsed = json.loads(f.readline())
    assert parsed["addresses"]["city"] == "NYC"
