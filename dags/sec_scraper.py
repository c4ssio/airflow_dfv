"""
sec_scraper.py

Airflow DAG: Download SEC EDGAR data (company submissions + company facts)
for a batch of companies and store raw JSON in S3 (preferred) or local disk.

Environment variables (optional):
- SEC_USER_AGENT: REQUIRED by SEC guidance; include a real email.
  Example: "drclive SEC scraper (you@drclive.net)"
- SEC_REQUESTS_PER_SECOND: default 5  (keep <= 10; be polite)
- SEC_TIMEOUT_SECONDS: default 30
- SEC_MAX_CIKS_PER_RUN: default 250
- SEC_START_CIK: default "" (if set, start from this CIK in the tickers list)

S3 (optional; if not set, saves locally under /tmp/sec_raw):
- SEC_S3_BUCKET
- SEC_S3_PREFIX: default "sec_raw"

Requires:
- requests
- (optional for S3) apache-airflow-providers-amazon, boto3
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, cast

from airflow import DAG
from airflow.decorators import task  # type: ignore
from airflow.exceptions import AirflowFailException
from airflow.utils.edgemodifier import Label

from scripts.sec_scraper.common import (
    SecScraperConfigError,
    Settings,
    load_settings as _load_settings,
    make_session as _session,
)
from scripts.sec_scraper.tasks.fetch_company_ciks import fetch_company_ciks

logger = logging.getLogger(__name__)

SEC_BASE = "https://data.sec.gov"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

def _settings() -> Settings:
    """Load settings and raise an Airflow-friendly exception on config errors."""
    try:
        return _load_settings(__file__)
    except SecScraperConfigError as e:
        raise AirflowFailException(str(e))

# All helpers moved to plugins/scripts/sec_scraper modules

# Removed duplicate helpers - use imports from common.py, storage.py, postgres/helpers.py instead

default_args = {
    "owner": "drclive",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sec_scraper",
    description="Download SEC EDGAR company submissions + facts JSON into raw storage",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",  # daily at 06:00
    catchup=False,
    max_active_runs=1,
    tags=["sec", "edgar", "finance"],
) as dag:

    @task
    def get_company_ciks() -> List[Dict[str, str]]:
        """
        Download the SEC's company_tickers.json and return a list of records:
          [{"cik": "...", "ticker": "...", "title": "..."}]
        """
        cfg = _settings()
        s = _session(cfg.user_agent)
        return fetch_company_ciks(cfg, s, TICKERS_URL)

    @task
    def fetch_and_store_companies(companies: List[Dict[str, str]]) -> Dict[str, Any]:
        """Process companies sequentially and store JSON files."""
        cfg = _settings()
        s = _session(cfg.user_agent)
        from scripts.sec_scraper.tasks.fetch_and_store_companies import (
            fetch_and_store_companies as _fetch_and_store,
        )
        return _fetch_and_store(cfg, s, companies, SEC_BASE)

    @task
    def summarize(summary: Dict[str, Any]) -> None:
        """Summarize the processing results."""
        # Handle both old format (list) and new format (summary dict)
        if isinstance(summary, dict) and "results_file" in summary:
            # New format: summary dict
            total_companies = summary.get("total_companies", 0)
            facts_downloaded = summary.get("facts_downloaded", 0)
            facts_skipped = summary.get("facts_skipped", 0)
            results_file = summary.get("results_file")
            logger.info(
                "Done. Processed %d companies, %d facts downloaded, %d facts skipped",
                total_companies,
                facts_downloaded,
                facts_skipped,
            )
            if results_file:
                logger.info("Results written to: %s", results_file)
        else:
            # Old format: list of results (for backward compatibility)
            results: List[Dict[str, Any]] = summary if isinstance(summary, list) else []
            stored_s3 = sum(1 for r in results if isinstance(r, dict) and r.get("stored") == "s3")
            stored_local = sum(1 for r in results if isinstance(r, dict) and r.get("stored") == "local")
            facts_downloaded = sum(1 for r in results if isinstance(r, dict) and r.get("facts_downloaded", True))
            facts_skipped = len(results) - facts_downloaded
            logger.info(
                "Done. Stored to S3: %d, stored locally: %d",
                stored_s3,
                stored_local,
            )
            logger.info(
                "Company facts downloaded: %d, skipped (no new filings): %d",
                facts_downloaded,
                facts_skipped,
            )
            if results:
                first_result = cast(List[Dict[str, Any]], results)[0] if results else {}
                logger.debug("Sample output: %s", json.dumps(first_result, indent=2))

    @task
    def ingest_to_postgres(summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert JSON files to NDJSON and load into PostgreSQL tables."""
        cfg = _settings()
        from scripts.sec_scraper.tasks.ingest_to_postgres import ingest_to_postgres as _ingest
        try:
            result = _ingest(cfg)
            logger.info(
                "Ingestion complete: %d CIKs processed, %d errors",
                result.get("processed", 0),
                result.get("errors", 0),
            )
            return result
        except RuntimeError as e:
            raise AirflowFailException(str(e))

    @task
    def validate_postgres_ingestion(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data integrity after PostgreSQL ingestion."""
        cfg = _settings()
        from scripts.sec_scraper.tasks.validate_postgres_ingestion import (
            validate_postgres_ingestion as _validate,
        )
        try:
            return _validate(cfg, ingest_result)
        except RuntimeError as e:
            raise AirflowFailException(str(e))

    companies = get_company_ciks()
    stored = fetch_and_store_companies(companies)
    ingested = ingest_to_postgres(stored)
    validated = validate_postgres_ingestion(ingested)

    # Task dependencies
    _ = companies >> Label("download SEC JSON + store raw") >> stored >> Label("ingest to PostgreSQL") >> ingested >> Label("validate") >> validated >> summarize(stored)  # type: ignore[operator]
