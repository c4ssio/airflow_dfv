"""Tests for postgres/deploy_migrations.py get_config env-var fallback."""
from __future__ import annotations

import pytest

from scripts.sec_scraper.postgres.deploy_migrations import get_config


def test_get_config_env_var_fallback(monkeypatch):
    """When POSTGRES_HOST is set, env vars are used instead of YAML."""
    monkeypatch.setenv("POSTGRES_HOST", "rds.example.com")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "mydb")
    monkeypatch.setenv("POSTGRES_USER", "admin")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")

    config = get_config()  # no config path needed
    assert config["host"] == "rds.example.com"
    assert config["port"] == 5433
    assert config["database"] == "mydb"
    assert config["user"] == "admin"
    assert config["password"] == "secret"
    assert config["schema"] == "public"


def test_get_config_env_var_defaults(monkeypatch):
    """When only POSTGRES_HOST is set, other values use sensible defaults."""
    monkeypatch.setenv("POSTGRES_HOST", "db.local")
    for k in ("POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_SCHEMA"):
        monkeypatch.delenv(k, raising=False)

    config = get_config()
    assert config["host"] == "db.local"
    assert config["port"] == 5432
    assert config["database"] == "sec_data"
    assert config["schema"] == "sec_raw"


def test_get_config_falls_back_to_yaml_when_no_env(tmp_path, monkeypatch):
    """When POSTGRES_HOST is not set, falls back to the YAML file."""
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    config_file = tmp_path / "postgres.yaml"
    config_file.write_text(
        "host: yaml-host\nport: 5432\ndatabase: testdb\nuser: u\npassword: p\n"
    )
    config = get_config(str(config_file))
    assert config["host"] == "yaml-host"
