"""Shared test fixtures and module stubs.

Stubs out heavy dependencies (airflow, yfinance, psycopg2) that are only
available inside the Docker environment so that pure-logic unit tests can
run on the host without installing the full Airflow stack.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock


def _ensure_stub(module_name: str, attrs: dict | None = None) -> None:
    """Register a lightweight stub module if the real one is not installed."""
    if module_name in sys.modules:
        return
    try:
        __import__(module_name)
    except ImportError:
        mod = types.ModuleType(module_name)
        if attrs:
            for k, v in attrs.items():
                setattr(mod, k, v)
        sys.modules[module_name] = mod


# --- Airflow stubs ---
_ensure_stub("airflow")
_ensure_stub("airflow.exceptions", {
    "AirflowFailException": type("AirflowFailException", (RuntimeError,), {}),
})
_ensure_stub("airflow.decorators", {"task": lambda f: f})
_ensure_stub("airflow.operators")
_ensure_stub("airflow.operators.python", {"get_current_context": lambda: {}})

# --- yfinance stub ---
_ensure_stub("yfinance", {"Ticker": MagicMock})

# --- psycopg2 stub (only needed if not installed) ---

class _FakeSQLIdentifier:
    def __init__(self, val: str = ""):
        self._val = val
    def format(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        return self
    def __str__(self) -> str:
        return self._val

_psycopg2_extensions_stub = types.ModuleType("psycopg2.extensions")
_psycopg2_extensions_stub.connection = type("connection", (), {})  # type: ignore[attr-defined]

_psycopg2_sql_stub = types.ModuleType("psycopg2.sql")
_psycopg2_sql_stub.SQL = staticmethod(lambda s: _FakeSQLIdentifier(s))  # type: ignore[attr-defined]
_psycopg2_sql_stub.Identifier = staticmethod(lambda s: _FakeSQLIdentifier(s))  # type: ignore[attr-defined]

_ensure_stub("psycopg2", {
    "connect": MagicMock,
    "extensions": _psycopg2_extensions_stub,
    "sql": _psycopg2_sql_stub,
})
_ensure_stub("psycopg2.extensions", {
    "connection": type("connection", (), {}),
})
_ensure_stub("psycopg2.sql", {
    "SQL": _psycopg2_sql_stub.SQL,
    "Identifier": _psycopg2_sql_stub.Identifier,
})
