from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests

from scripts.sec_scraper.common import Settings, get_json

logger = logging.getLogger(__name__)


def fetch_company_ciks(cfg: Settings, session: requests.Session, tickers_url: str) -> List[Dict[str, str]]:
    """Download the SEC's company_tickers.json and return normalized rows."""
    data: Dict[str, Any] = get_json(session, tickers_url, cfg.timeout_s, cfg.rps)

    rows: List[Dict[str, str]] = []
    for _, v in data.items():
        cik = str(v.get("cik_str", "")).strip()
        ticker = str(v.get("ticker", "")).strip()
        title = str(v.get("title", "")).strip()
        if cik and ticker:
            rows.append({"cik": cik, "ticker": ticker, "title": title})

    rows.sort(key=lambda r: int(r["cik"]))

    if cfg.start_cik:
        rows = [r for r in rows if int(r["cik"]) >= int(cfg.start_cik)]

    logger.info("Found %d total companies in SEC tickers file", len(rows))
    logger.info("max_ciks limit: %d", cfg.max_ciks)
    result = rows[: cfg.max_ciks]
    logger.info("Returning %d companies (limited by max_ciks)", len(result))
    return result

