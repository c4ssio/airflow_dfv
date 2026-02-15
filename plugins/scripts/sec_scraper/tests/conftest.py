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
_ensure_stub("psycopg2")
