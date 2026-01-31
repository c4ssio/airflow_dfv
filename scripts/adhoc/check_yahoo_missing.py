#!/usr/bin/env python3
"""
Diagnostic: why do some tracked tickers have no price in ticker_prices_daily?

Fetches the list of tickers that are in submissions_ticker_mapping (latest ingest,
is_likely_common_stock) but not in ticker_prices_daily, then calls Yahoo Finance
(yfinance) for each and classifies the response.

Run from repo root (need plugins on path and Postgres reachable):
  PYTHONPATH=plugins python scripts/adhoc/check_yahoo_missing.py

From host with Docker Postgres: use POSTGRES_CONFIG_PATH pointing to a config
with host localhost (or port-forward). Optional env: SEC_PRICE_DATE (YYYY-MM-DD).
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import yfinance as yf

if __name__ == "__main__":
    _here = os.path.abspath(os.path.dirname(__file__))
    _plugins = os.path.abspath(os.path.join(_here, "..", "..", "plugins"))
    if _plugins not in sys.path:
        sys.path.insert(0, _plugins)

from scripts.sec_scraper.postgres.helpers import get_postgres_config, get_postgres_connection


def classify_yahoo_response(ticker: str, price_date: str) -> str:
    """Call Yahoo Finance (yfinance); return "has_data" | "empty" | "error:...". """
    try:
        t = yf.Ticker(ticker)
        end_d = datetime.strptime(price_date, "%Y-%m-%d").date() + timedelta(days=1)
        end_str = end_d.isoformat()
        hist = t.history(start=price_date, end=end_str, auto_adjust=True, timeout=10)
    except Exception as e:
        return f"error:{type(e).__name__}"

    if hist is None or hist.empty:
        return "empty"
    return "has_data"


def get_missing_tickers(conn: Any, schema: str) -> List[str]:
    """Tickers that are tracked (latest ingest, is_likely_common_stock) but not in ticker_prices_daily."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(ingest_date) FROM {schema}.submissions")
        latest = cur.fetchone()[0]
        if not latest:
            return []
        latest_str = str(latest)
        cur.execute(
            f"""
            SELECT DISTINCT m.ticker
            FROM {schema}.submissions_ticker_mapping m
            WHERE m.ingest_date = %s
              AND m.is_likely_common_stock = TRUE
              AND m.ticker IS NOT NULL AND m.ticker <> ''
              AND NOT EXISTS (
                SELECT 1 FROM {schema}.ticker_prices_daily p
                WHERE p.ticker = m.ticker
              )
            ORDER BY m.ticker
            """,
            (latest_str,),
        )
        return [r[0] for r in cur.fetchall() if r and r[0]]
    finally:
        cur.close()


def main() -> None:
    price_date = os.environ.get("SEC_PRICE_DATE", "").strip()
    if not price_date:
        price_date = datetime.now(timezone.utc).date().isoformat()

    config_path = os.environ.get(
        "POSTGRES_CONFIG_PATH",
        os.path.join(os.path.dirname(__file__), "..", "..", "config", "postgres.yaml"),
    )
    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    config = get_postgres_config(config_path)
    schema = config.get("schema", "sec_raw")
    conn = get_postgres_connection(config, schema)
    try:
        missing = get_missing_tickers(conn, schema)
    finally:
        conn.close()

    if not missing:
        print("No missing tickers (all tracked tickers have at least one price).")
        return

    print(f"Checking {len(missing)} missing tickers at Yahoo Finance for price_date={price_date} ...")
    by_result: Dict[str, List[str]] = defaultdict(list)
    for i, ticker in enumerate(missing):
        kind = classify_yahoo_response(ticker, price_date)
        by_result[kind].append(ticker)
        if (i + 1) % 20 == 0:
            print(f"  ... {i + 1}/{len(missing)}", flush=True)

    print()
    print("Summary (why no price in our DB):")
    print("- has_data: Yahoo returned history for this date -> we could have stored it (unexpected).")
    print("- empty:    Yahoo returned no rows (symbol not found, or no trade on this date).")
    print("- error:... Request failed (timeout, etc.).")
    print()
    for kind in ["has_data", "empty"]:
        tickers = by_result.get(kind, [])
        if tickers:
            print(f"  {kind}: {len(tickers)}")
            if len(tickers) <= 15:
                print(f"    -> {', '.join(tickers)}")
            else:
                print(f"    -> {', '.join(tickers[:10])} ... and {len(tickers) - 10} more")
    for k, v in sorted(by_result.items()):
        if k.startswith("error:"):
            print(f"  {k}: {len(v)}")
            if len(v) <= 5:
                print(f"    -> {', '.join(v)}")

    not_available_count = len(by_result.get("empty", [])) + sum(
        len(v) for k, v in by_result.items() if k.startswith("error:")
    )
    if not_available_count:
        print()
        print(f"Conclusion: {not_available_count} tickers have no usable data from Yahoo (empty or error).")
        print("So for those, 'missing' is because Yahoo doesn't have them or no data for this date.")
    has_data = by_result.get("has_data", [])
    if has_data:
        print()
        print(f"Note: {len(has_data)} ticker(s) returned data from Yahoo but are missing in our DB (pipeline may have failed earlier).")


if __name__ == "__main__":
    main()
