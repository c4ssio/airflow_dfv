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

import gc
import json
import logging
import os
import resource
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
import shutil
from typing import Any, Dict, List, Optional, Set

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.edgemodifier import Label

from scripts.sec_scraper.common import (
    SecScraperConfigError,
    Settings,
    get_json,
    get_memory_mb,
    load_settings as _load_settings,
    make_session as _session,
    pad_cik,
)
from scripts.sec_scraper.postgres.helpers import (
    get_postgres_config,
    get_postgres_connection,
    load_ndjson_batch_to_postgres,
    upsert_metric_metadata,
    write_ndjson_file,
)
from scripts.sec_scraper.storage import (
    find_existing_data,
    get_most_recent_filing_date,
    read_metadata,
    s3_key,
    write_bytes,
    write_metadata,
)
from scripts.sec_scraper.tasks.fetch_company_ciks import fetch_company_ciks

logger = logging.getLogger(__name__)

try:
    # Only needed if you set SEC_S3_BUCKET
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
except Exception:  # pragma: no cover
    S3Hook = None  # type: ignore

try:
    import psycopg2
    from psycopg2 import sql as psycopg2_sql
    from psycopg2.extras import execute_values
    import yaml
    from pathlib import Path
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    psycopg2_sql = None  # type: ignore
    execute_values = None  # type: ignore
    yaml = None  # type: ignore

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
    def fetch_and_store_all_companies(companies: List[Dict[str, str]]) -> Dict[str, Any]:
        """Process all companies sequentially and store JSON files."""
        cfg = _settings()
        s = _session(cfg.user_agent)
        from scripts.sec_scraper.tasks.fetch_and_store_all_companies import (
            fetch_and_store_all_companies as _fetch_and_store,
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
            results = summary if isinstance(summary, list) else []
            stored_s3 = sum(1 for r in results if r.get("stored") == "s3")
            stored_local = sum(1 for r in results if r.get("stored") == "local")
            facts_downloaded = sum(1 for r in results if r.get("facts_downloaded", True))
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
                logger.debug("Sample output: %s", json.dumps(results[0], indent=2))

    @task
    def ingest_to_postgres(summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert JSON files to NDJSON and load into PostgreSQL tables.
        Processes all available CIKs serially.
        """
        cfg = _settings()

        # Load PostgreSQL config
        try:
            postgres_config = get_postgres_config(cfg.postgres_config_path)
        except Exception as e:
            logger.error("Failed to load PostgreSQL config: %s", e)
            raise AirflowFailException(f"PostgreSQL config error: {e}")

        # Delegate to shared ingestion implementation for testability
        from scripts.sec_scraper.tasks.ingest_to_postgres import ingest_all_ciks

        class _LoadFns:
            @staticmethod
            def get_connection(config: Dict[str, Any], schema: str):
                return get_postgres_connection(config, schema)

            @staticmethod
            def load_ndjson_batch(
                conn: Any,
                table_name: str,
                ndjson_paths: List[str],
                cik_list: List[str],
                ingest_date: str,
                schema: str,
            ) -> int:
                return load_ndjson_batch_to_postgres(
                    conn, table_name, ndjson_paths, cik_list, ingest_date, schema
                )

        result = ingest_all_ciks(
            cfg=cfg,
            postgres_config=postgres_config,
            load_fn=_LoadFns,
            upsert_metric_metadata_fn=upsert_metric_metadata,
        )
        logger.info(
            "Ingestion complete via shared implementation: %d CIKs processed, %d errors",
            result.get("processed", 0),
            result.get("errors", 0),
        )
        return result

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
    stored = fetch_and_store_all_companies(companies)
    ingested = ingest_to_postgres(stored)
    validated = validate_postgres_ingestion(ingested)

    companies >> Label("download SEC JSON + store raw") >> stored >> Label("ingest to PostgreSQL") >> ingested >> Label("validate") >> validated >> summarize(stored)
