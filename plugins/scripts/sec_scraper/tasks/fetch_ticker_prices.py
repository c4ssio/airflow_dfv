from __future__ import annotations

import csv
import io
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yfinance as yf

from scripts.sec_scraper.common import Settings, rate_limit
from scripts.sec_scraper.postgres.helpers import get_postgres_config, get_postgres_connection

_DEFAULT_BACKFILL_DAYS = 365

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceBar:
    ticker: str
    price_date: str  # YYYY-MM-DD
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[int]
    currency: Optional[str]
    source: str


def _today_utc_date_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _to_float(v: str) -> Optional[float]:
    v = v.strip()
    if not v or v == "N/A":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: str) -> Optional[int]:
    v = v.strip()
    if not v or v == "N/A":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _iter_stooq_daily_csv_rows(csv_text: str) -> Iterable[Dict[str, str]]:
    """
    Stooq daily CSV format:
      Date,Open,High,Low,Close,Volume
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        if not row:
            continue
        yield {k.strip(): (v.strip() if isinstance(v, str) else "") for k, v in row.items() if k}


def _stooq_symbol_for_ticker(ticker: str) -> str:
    # Stooq uses e.g. "aapl.us". This is a best-effort default for US equities.
    return f"{ticker.lower()}.us"


def _fetch_stooq_bar_for_date(
    session: requests.Session,
    *,
    ticker: str,
    price_date: str,
    timeout_s: int,
    rps: float,
) -> Optional[PriceBar]:
    """
    Fetch daily bars and pick the last bar with Date <= price_date.
    Returns None if the provider has no data for the ticker.
    """
    sym = _stooq_symbol_for_ticker(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"

    rate_limit(rps)
    resp = session.get(url, timeout=timeout_s)
    if resp.status_code != 200:
        logger.warning("Price fetch failed ticker=%s status=%s", ticker, resp.status_code)
        return None

    text = resp.text.strip()
    if not text or "Date,Open,High,Low,Close,Volume" not in text:
        # Stooq sometimes returns "No data" HTML/text for unknown symbols.
        return None

    last_row: Optional[Dict[str, str]] = None
    for row in _iter_stooq_daily_csv_rows(text):
        d = row.get("Date", "")
        if not d:
            continue
        if d <= price_date:
            last_row = row

    if not last_row:
        return None

    return PriceBar(
        ticker=ticker,
        price_date=last_row.get("Date", price_date),
        open=_to_float(last_row.get("Open", "")),
        high=_to_float(last_row.get("High", "")),
        low=_to_float(last_row.get("Low", "")),
        close=_to_float(last_row.get("Close", "")),
        volume=_to_int(last_row.get("Volume", "")),
        currency=None,
        source="stooq",
    )


def _fetch_yahoo_bar_for_date(
    *,
    ticker: str,
    price_date: str,
    rps: float,
) -> Optional[PriceBar]:
    """
    Fetch daily bar for ticker on price_date via Yahoo Finance (yfinance).
    Uses ticker as-is so OTC/OTCMKTS symbols (e.g. ABCP) work.
    Returns None if no data.
    """
    rate_limit(rps)
    try:
        t = yf.Ticker(ticker)
        # Request a short window so we get at most one row for price_date
        end_d = datetime.strptime(price_date, "%Y-%m-%d").date() + timedelta(days=1)
        end_str = end_d.isoformat()
        hist = t.history(start=price_date, end=end_str, auto_adjust=True, timeout=10)
    except Exception as e:
        logger.debug("Yahoo fetch failed ticker=%s: %s", ticker, e)
        return None
    if hist is None or hist.empty:
        return None
    # Take the last row (closest to price_date)
    row = hist.iloc[-1]
    row_date = row.name
    if hasattr(row_date, "date"):
        bar_date = row_date.date().isoformat()
    else:
        bar_date = price_date
    open_v = float(row["Open"]) if row.get("Open") is not None and str(row["Open"]) != "nan" else None
    high_v = float(row["High"]) if row.get("High") is not None and str(row["High"]) != "nan" else None
    low_v = float(row["Low"]) if row.get("Low") is not None and str(row["Low"]) != "nan" else None
    close_v = float(row["Close"]) if row.get("Close") is not None and str(row["Close"]) != "nan" else None
    vol = row.get("Volume")
    if vol is not None and str(vol) != "nan":
        try:
            vol = int(vol)
        except (ValueError, TypeError):
            vol = None
    else:
        vol = None
    return PriceBar(
        ticker=ticker,
        price_date=bar_date,
        open=open_v,
        high=high_v,
        low=low_v,
        close=close_v,
        volume=vol,
        currency=None,
        source="yahoo",
    )


def _get_last_price_date(conn: Any, schema: str) -> Optional[str]:
    """Return the most recent price_date in ticker_prices_daily, or None."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(price_date) FROM {schema}.ticker_prices_daily")
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        return None
    finally:
        cur.close()


def _fetch_yahoo_bars_for_range(
    *,
    ticker: str,
    start_date: str,
    end_date: str,
    rps: float,
) -> List[PriceBar]:
    """Fetch daily bars for a date range via yfinance. Returns all bars in [start_date, end_date]."""
    rate_limit(rps)
    try:
        t = yf.Ticker(ticker)
        # yfinance end is exclusive, so add 1 day
        end_d = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
        hist = t.history(start=start_date, end=end_d.isoformat(), auto_adjust=True, timeout=30)
    except Exception as e:
        logger.debug("Yahoo range fetch failed ticker=%s: %s", ticker, e)
        return []
    if hist is None or hist.empty:
        return []

    bars: List[PriceBar] = []
    for idx, row in hist.iterrows():
        if hasattr(idx, "date"):
            bar_date = idx.date().isoformat()
        else:
            bar_date = str(idx)[:10]

        open_v = float(row["Open"]) if row.get("Open") is not None and str(row["Open"]) != "nan" else None
        high_v = float(row["High"]) if row.get("High") is not None and str(row["High"]) != "nan" else None
        low_v = float(row["Low"]) if row.get("Low") is not None and str(row["Low"]) != "nan" else None
        close_v = float(row["Close"]) if row.get("Close") is not None and str(row["Close"]) != "nan" else None
        vol = row.get("Volume")
        if vol is not None and str(vol) != "nan":
            try:
                vol = int(vol)
            except (ValueError, TypeError):
                vol = None
        else:
            vol = None

        bars.append(PriceBar(
            ticker=ticker,
            price_date=bar_date,
            open=open_v,
            high=high_v,
            low=low_v,
            close=close_v,
            volume=vol,
            currency=None,
            source="yahoo",
        ))
    return bars


def _select_tickers_for_latest_ingest(conn: Any, schema: str, *, limit: int) -> Tuple[str, List[str]]:
    """
    Returns (latest_ingest_date, tickers).
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(ingest_date) FROM {schema}.submissions")
        latest = cur.fetchone()[0]
        if not latest:
            return ("", [])
        latest_str = str(latest)

        cur.execute(
            f"""
            SELECT DISTINCT ticker
            FROM {schema}.submissions_ticker_mapping
            WHERE ingest_date = %s
              AND is_likely_common_stock = TRUE
              AND ticker IS NOT NULL
              AND ticker <> ''
            ORDER BY ticker
            LIMIT %s
            """,
            (latest_str, limit),
        )
        tickers = [r[0] for r in cur.fetchall() if r and r[0]]
        return (latest_str, tickers)
    finally:
        cur.close()


def _upsert_price_bars(conn: Any, schema: str, bars: List[PriceBar]) -> int:
    if not bars:
        return 0
    cur = conn.cursor()
    try:
        cur.executemany(
            f"""
            INSERT INTO {schema}.ticker_prices_daily
                (ticker, price_date, open, high, low, close, volume, currency, source, fetched_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (ticker, price_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                currency = EXCLUDED.currency,
                source = EXCLUDED.source,
                fetched_at = CURRENT_TIMESTAMP
            """,
            [
                (
                    b.ticker,
                    b.price_date,
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    b.volume,
                    b.currency,
                    b.source,
                )
                for b in bars
            ],
        )
        conn.commit()
        return len(bars)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def fetch_ticker_prices(
    cfg: Settings,
    session: requests.Session,
    *,
    price_date: Optional[str] = None,
    backfill: bool = False,
) -> Dict[str, Any]:
    """
    Fetch daily ticker prices and upsert them into PostgreSQL.

    Provider: Yahoo Finance (yfinance). Tickers are used as-is so OTC/OTCMKTS (e.g. ABCP) work.
    Tickers are sourced from the latest `sec_raw.submissions_ticker_mapping` ingest_date.

    When ``backfill=True`` the function queries ``ticker_prices_daily`` for the
    most recent ``price_date`` and fetches all trading days from the day after
    that date through ``price_date`` (the target end date).  If the table is
    empty it defaults to fetching the last ``_DEFAULT_BACKFILL_DAYS`` days.
    """
    price_date = price_date or os.environ.get("SEC_PRICE_DATE", "").strip() or _today_utc_date_str()
    max_tickers = int(os.environ.get("SEC_MAX_TICKERS_PER_RUN", str(cfg.max_ciks)))

    postgres_config = get_postgres_config(cfg.postgres_config_path)
    schema = postgres_config.get("schema", "sec_raw")
    conn = get_postgres_connection(postgres_config, schema)
    try:
        latest_ingest_date, tickers = _select_tickers_for_latest_ingest(conn, schema, limit=max_tickers)
        if not tickers:
            logger.info("No tickers found for price fetch (latest_ingest_date=%s)", latest_ingest_date)
            return {
                "price_date": price_date,
                "latest_ingest_date": latest_ingest_date,
                "tickers": 0,
                "fetched": 0,
                "stored": 0,
            }

        # Determine date range
        if backfill:
            last_price = _get_last_price_date(conn, schema)
            if last_price:
                start_date = (datetime.strptime(last_price, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
            else:
                start_date = (datetime.strptime(price_date, "%Y-%m-%d").date() - timedelta(days=_DEFAULT_BACKFILL_DAYS)).isoformat()

            if start_date > price_date:
                logger.info("Backfill: nothing to do — last_price_date=%s >= price_date=%s", last_price, price_date)
                return {
                    "price_date": price_date,
                    "latest_ingest_date": latest_ingest_date,
                    "tickers": len(tickers),
                    "fetched": 0,
                    "stored": 0,
                    "backfill": True,
                    "backfill_start": start_date,
                    "source": "yahoo",
                }

            logger.info("Backfill: fetching range %s → %s for %d tickers", start_date, price_date, len(tickers))
            fetched_bars: List[PriceBar] = []
            missing = 0
            for t in tickers:
                bars = _fetch_yahoo_bars_for_range(
                    ticker=t,
                    start_date=start_date,
                    end_date=price_date,
                    rps=cfg.rps,
                )
                if not bars:
                    missing += 1
                else:
                    fetched_bars.extend(bars)

            stored = _upsert_price_bars(conn, schema, fetched_bars)
            logger.info(
                "Backfill prices: tickers=%d bars=%d missing=%d stored=%d range=%s→%s",
                len(tickers), len(fetched_bars), missing, stored, start_date, price_date,
            )
            return {
                "price_date": price_date,
                "latest_ingest_date": latest_ingest_date,
                "tickers": len(tickers),
                "fetched": len(fetched_bars),
                "missing": missing,
                "stored": stored,
                "backfill": True,
                "backfill_start": start_date,
                "source": "yahoo",
            }

        # Single-date fetch (original behaviour)
        fetched_bars = []
        missing = 0
        for t in tickers:
            bar = _fetch_yahoo_bar_for_date(
                ticker=t,
                price_date=price_date,
                rps=cfg.rps,
            )
            if bar is None:
                missing += 1
                continue
            fetched_bars.append(bar)

        stored = _upsert_price_bars(conn, schema, fetched_bars)
        logger.info(
            "Ticker prices: tickers=%d fetched=%d missing=%d stored=%d price_date=%s",
            len(tickers),
            len(fetched_bars),
            missing,
            stored,
            price_date,
        )
        return {
            "price_date": price_date,
            "latest_ingest_date": latest_ingest_date,
            "tickers": len(tickers),
            "fetched": len(fetched_bars),
            "missing": missing,
            "stored": stored,
            "source": "yahoo",
        }
    finally:
        conn.close()

