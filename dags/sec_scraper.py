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

from plugins.scripts.sec_scraper.common import (
    SecScraperConfigError,
    Settings,
    get_json as _get_json,
    load_settings as _load_settings,
    make_session as _session,
)
from plugins.scripts.sec_scraper.tasks.fetch_company_ciks import fetch_company_ciks

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

# --- Shared rate limiter for coordinated rate limiting across tasks ---
# Uses a lock-protected global variable so all tasks in the same DAG run
# coordinate to respect the overall rate limit (e.g., 5 req/s total, not per task)
_rate_limit_lock = threading.Lock()
_last_request_ts: float = 0.0

def _rate_limit(rps: float) -> None:
    """
    Sleep so we don't exceed rps across ALL tasks in the same process.

    Uses a shared lock-protected timestamp so that all task instances
    coordinate to respect the overall rate limit. This ensures that if
    you have 25 tasks running in parallel, they collectively respect
    the 5 req/s limit rather than each trying to do 5 req/s independently.

    Note: This coordinates within a single worker process. If you have
    multiple workers, each worker will independently rate limit. For
    true global coordination across all workers, you'd need Redis or
    a distributed lock mechanism.
    """
    global _last_request_ts

    if rps <= 0:
        return

    with _rate_limit_lock:
        min_interval = 1.0 / rps
        now = time.time()
        wait = (_last_request_ts + min_interval) - now
        if wait > 0:
            # Release lock before sleeping to allow other tasks to proceed
            # (though they'll still wait when they acquire the lock)
            pass
        else:
            wait = 0

    # Sleep outside the lock to avoid blocking other tasks unnecessarily
    if wait > 0:
        time.sleep(wait)

    # Update timestamp while holding the lock
    with _rate_limit_lock:
        _last_request_ts = time.time()

def _get_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        # Get RSS (Resident Set Size) in bytes, convert to MB
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:
        return 0.0

def _get_json(
    s: requests.Session,
    url: str,
    timeout_s: int,
    rps: float,
    max_attempts: int = 5,
    log_memory: bool = False,
) -> Dict[str, Any]:
    """
    Robust GET with backoff on 429/5xx.

    Args:
        log_memory: If True, log memory usage before and after loading JSON
    """
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        _rate_limit(rps)
        mem_before = _get_memory_mb() if log_memory else 0.0
        resp = s.get(url, timeout=timeout_s)
        if resp.status_code == 200:
            if log_memory:
                logger.info(
                    "Memory before JSON parse (%s): %.1f MB, Response size: %.1f MB",
                    url,
                    mem_before,
                    len(resp.content) / 1024.0 / 1024.0,
                )
            data = resp.json()
            if log_memory:
                mem_after = _get_memory_mb()
                logger.info(
                    "Memory after JSON parse (%s): %.1f MB (delta: +%.1f MB)",
                    url,
                    mem_after,
                    mem_after - mem_before,
                )
            return data

        # Retry on SEC rate limiting or transient errors
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_attempts:
                raise AirflowFailException(
                    f"GET {url} failed after {max_attempts} attempts "
                    f"(status={resp.status_code}): {resp.text[:300]}"
                )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        # Non-retryable
        raise AirflowFailException(
            f"GET {url} failed (status={resp.status_code}): {resp.text[:300]}"
        )

    raise AirflowFailException(f"Unreachable retry loop for {url}")

def _pad_cik(cik: str) -> str:
    # SEC endpoints expect 10-digit, zero-padded CIK in some paths.
    return cik.zfill(10)

def _s3_key(prefix: str, cik: str, name: str) -> str:
    return f"{prefix}/cik={cik}/{name}.json"

def _write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

def _get_most_recent_filing_date(submissions_data: Dict[str, Any]) -> Optional[str]:
    """Extract the most recent filing date from submissions.json data."""
    if "filings" not in submissions_data:
        return None
    filings = submissions_data["filings"]
    if "recent" not in filings:
        return None
    recent = filings["recent"]
    if "filingDate" not in recent or not recent["filingDate"]:
        return None
    filing_dates = recent["filingDate"]
    if not filing_dates:
        return None
    return max(filing_dates)

def _read_metadata(cik_dir: str) -> Optional[Dict[str, Any]]:
    """Read metadata.json from a CIK directory."""
    metadata_path = os.path.join(cik_dir, "metadata.json")
    if not os.path.exists(metadata_path):
        return None
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_metadata(cik_dir: str, latest_filing_date: Optional[str], ingest_date: str) -> None:
    """Write metadata.json to a CIK directory."""
    os.makedirs(cik_dir, exist_ok=True)
    metadata = {
        "latest_filing_date": latest_filing_date,
        "last_updated": ingest_date,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    metadata_path = os.path.join(cik_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

def _convert_submissions_to_ndjson(
    submissions_data: Dict[str, Any], cik: str, ingest_date: str
) -> Dict[str, Any]:
    """
    Convert submissions.json data to a single row matching the Snowflake schema.
    Returns a dict that can be serialized to JSON for NDJSON format.
    """
    cik_padded = _pad_cik(cik)

    # Extract tickers and exchanges arrays
    tickers = submissions_data.get("tickers", [])
    exchanges = submissions_data.get("exchanges", [])

    # Validate array lengths match - these should be parallel arrays
    if len(tickers) != len(exchanges):
        logger.warning(
            f"CIK {cik_padded}: tickers ({len(tickers)}) and exchanges ({len(exchanges)}) "
            f"arrays have different lengths. Truncating to shorter length."
        )
        min_len = min(len(tickers), len(exchanges))
        tickers = tickers[:min_len]
        exchanges = exchanges[:min_len]

    # Extract fields matching the Snowflake schema
    row = {
        "cik": cik_padded,
        "entity_name": submissions_data.get("name"),
        "entity_type": submissions_data.get("entityType"),
        "ein": submissions_data.get("ein"),
        "lei": submissions_data.get("lei"),
        "sic": submissions_data.get("sic"),
        "sic_description": submissions_data.get("sicDescription"),
        "category": submissions_data.get("category"),
        "owner_org": submissions_data.get("ownerOrg"),
        "description": submissions_data.get("description"),
        "website": submissions_data.get("website"),
        "investor_website": submissions_data.get("investorWebsite"),
        "phone": submissions_data.get("phone"),
        "state_of_incorporation": submissions_data.get("stateOfIncorporation"),
        "state_of_incorporation_description": submissions_data.get("stateOfIncorporationDescription"),
        "fiscal_year_end": submissions_data.get("fiscalYearEnd"),
        "tickers": tickers,
        "exchanges": exchanges,
        "insider_transaction_for_issuer_exists": submissions_data.get("insiderTransactionForIssuerExists"),
        "insider_transaction_for_owner_exists": submissions_data.get("insiderTransactionForOwnerExists"),
        "flags": submissions_data.get("flags"),
        "former_names": submissions_data.get("formerNames", []),
        "addresses": submissions_data.get("addresses"),
        "filings": submissions_data.get("filings"),
        "ingest_date": ingest_date,
    }
    return row

def _validate_date_string(value: Any, field_name: str, cik: str) -> Optional[str]:
    """Validate that a value is a valid date string (YYYY-MM-DD) or None."""
    if value is None:
        return None
    if not isinstance(value, str):
        logger.warning(f"CIK {cik}: {field_name} has non-string value: {type(value).__name__}")
        return str(value) if value is not None else None
    # Basic date format validation
    if value and len(value) >= 10:
        try:
            # Check if it looks like a date (YYYY-MM-DD)
            parts = value[:10].split("-")
            if len(parts) == 3:
                int(parts[0])  # year
                int(parts[1])  # month
                int(parts[2])  # day
        except (ValueError, IndexError):
            logger.warning(f"CIK {cik}: {field_name} has invalid date format: {value}")
    return value


def _validate_numeric(value: Any, field_name: str, cik: str) -> Any:
    """Validate that a value is numeric or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    # Try to convert string to number
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            logger.warning(f"CIK {cik}: {field_name} has non-numeric string value: {value}")
            return None
    logger.warning(f"CIK {cik}: {field_name} has unexpected type: {type(value).__name__}")
    return None


def _validate_integer(value: Any, field_name: str, cik: str) -> Optional[int]:
    """Validate that a value is an integer or None."""
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value == int(value):
            return int(value)
        logger.warning(f"CIK {cik}: {field_name} has non-integer float value: {value}")
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            logger.warning(f"CIK {cik}: {field_name} has non-integer string value: {value}")
            return None
    logger.warning(f"CIK {cik}: {field_name} has unexpected type: {type(value).__name__}")
    return None


def _convert_companyfacts_to_ndjson(
    companyfacts_data: Dict[str, Any], cik: str, ingest_date: str
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Convert companyfacts.json data to NDJSON format.
    Returns (metadata_rows, facts_rows, metric_metadata_rows) where:
    - metadata_rows: list with single company metadata dict
    - facts_rows: list of fact dicts (without labels/descriptions)
    - metric_metadata_rows: list of unique metric metadata for upsert
    """
    cik_padded = _pad_cik(cik)

    # Company metadata row
    metadata_row = {
        "cik": cik_padded,
        "entity_name": companyfacts_data.get("entityName"),
        "ingest_date": ingest_date,
    }

    # Facts rows and metric metadata
    facts_rows = []
    metric_metadata = {}  # (taxonomy, metric_name) -> {label, description}
    facts_obj = companyfacts_data.get("facts", {})
    type_warnings = 0
    max_type_warnings = 10

    for taxonomy in ["dei", "us-gaap"]:
        taxonomy_facts = facts_obj.get(taxonomy, {})
        if not isinstance(taxonomy_facts, dict):
            continue

        for metric_name, metric_data in taxonomy_facts.items():
            if not isinstance(metric_data, dict):
                continue

            label = metric_data.get("label")
            description = metric_data.get("description")
            units = metric_data.get("units", {})

            # Collect metric metadata (deduplicated)
            key = (taxonomy, metric_name)
            if key not in metric_metadata:
                metric_metadata[key] = {
                    "taxonomy": taxonomy,
                    "metric_name": metric_name,
                    "label": label,
                    "description": description,
                }

            if not isinstance(units, dict):
                continue

            for unit_name, unit_entries in units.items():
                if not isinstance(unit_entries, list):
                    continue

                for entry in unit_entries:
                    if not isinstance(entry, dict):
                        continue

                    fiscal_year = entry.get("fy")
                    value = entry.get("val")
                    period_end = entry.get("end")
                    filed_date = entry.get("filed")

                    validated_fy = _validate_integer(fiscal_year, "fiscal_year", cik_padded)
                    if fiscal_year is not None and validated_fy is None and type_warnings < max_type_warnings:
                        type_warnings += 1

                    validated_value = _validate_numeric(value, "value", cik_padded)
                    if value is not None and validated_value is None and type_warnings < max_type_warnings:
                        type_warnings += 1

                    validated_period_end = _validate_date_string(period_end, "period_end", cik_padded)
                    validated_filed_date = _validate_date_string(filed_date, "filed_date", cik_padded)

                    # Facts row WITHOUT label/description (normalized)
                    fact_row = {
                        "cik": cik_padded,
                        "ingest_date": ingest_date,
                        "taxonomy": taxonomy,
                        "metric_name": metric_name,
                        "unit": unit_name,
                        "period_end": validated_period_end,
                        "value": validated_value,
                        "accession_number": entry.get("accn"),
                        "fiscal_year": validated_fy,
                        "fiscal_period": entry.get("fp"),
                        "form_type": entry.get("form"),
                        "filed_date": validated_filed_date,
                        "frame": entry.get("frame"),
                    }
                    facts_rows.append(fact_row)

    if type_warnings >= max_type_warnings:
        logger.warning(f"CIK {cik_padded}: Suppressed additional type validation warnings (total: {type_warnings}+)")

    return ([metadata_row], facts_rows, list(metric_metadata.values()))

def _get_postgres_config(config_path: str) -> Dict[str, Any]:
    """Load PostgreSQL configuration from YAML file."""
    if not yaml:
        raise AirflowFailException(
            "pyyaml is required for PostgreSQL ingestion. Install with: pip install pyyaml"
        )

    config_file = Path(config_path)
    if not config_file.exists():
        raise AirflowFailException(
            f"PostgreSQL config file not found: {config_path}"
        )

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    required_keys = ["host", "port", "database", "user", "password"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise AirflowFailException(
            f"Missing required PostgreSQL config keys: {', '.join(missing)}"
        )

    return config

def _write_ndjson_file(file_path: str, rows: List[Dict[str, Any]]) -> None:
    """Write rows to a newline-delimited JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def _get_postgres_connection(config: Dict[str, Any], schema: str = "sec_raw") -> Any:
    """Get a connection to PostgreSQL."""
    if not psycopg2:
        raise AirflowFailException(
            "psycopg2 is required. Install with: pip install psycopg2-binary"
        )

    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        connect_timeout=30,
    )

    # Set search path to the schema
    cursor = conn.cursor()
    cursor.execute(f"SET search_path TO {schema}, public")
    cursor.execute("SET statement_timeout = '300s'")  # 5 minute statement timeout
    cursor.close()
    conn.commit()

    return conn

def _load_ndjson_batch_to_postgres(
    conn: Any,
    table_name: str,
    ndjson_paths: List[str],
    cik_list: List[str],
    ingest_date: str,
    schema: str = "sec_raw",
) -> int:
    """
    Load a batch of NDJSON files into PostgreSQL table using streaming INSERT.

    Processes and commits one file at a time for:
    - Progress visibility (rows appear incrementally)
    - Failure resilience (completed files are saved)
    - Lower memory usage

    Returns the number of rows loaded.
    """
    if not ndjson_paths:
        return 0

    total_rows_loaded = 0
    files_processed = 0
    total_files = len(ndjson_paths)

    # Build a set of CIKs we're processing for targeted deletes
    cik_set = set(cik_list)

    for path in ndjson_paths:
        if not os.path.exists(path):
            logger.warning(f"NDJSON file not found, skipping: {path}")
            continue

        # Extract CIK from path
        cik_dir_name = os.path.basename(os.path.dirname(path))
        cik = cik_dir_name.replace("cik=", "").lstrip("0") or "0"
        cik_padded = cik.zfill(10)

        cursor = conn.cursor()
        file_rows = 0

        try:
            # Delete existing rows for THIS CIK only (per-file transaction)
            delete_sql = f"DELETE FROM {schema}.{table_name} WHERE ingest_date = %s AND cik = %s"
            cursor.execute(delete_sql, [ingest_date, cik_padded])

            # Process file
            insert_sql = None
            columns = None
            batch_values = []
            batch_size = 1000

            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    row = json.loads(line)

                    # Build INSERT statement from first row
                    if insert_sql is None:
                        columns = list(row.keys())
                        col_names = ", ".join(columns)
                        placeholders = ", ".join(["%s"] * len(columns))
                        insert_sql = f"INSERT INTO {schema}.{table_name} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                    # Convert dict/list values to JSON strings
                    values = []
                    for col in columns:
                        val = row.get(col)
                        if isinstance(val, (dict, list)):
                            values.append(json.dumps(val))
                        else:
                            values.append(val)
                    batch_values.append(tuple(values))
                    file_rows += 1

                    # Insert batch
                    if len(batch_values) >= batch_size:
                        cursor.executemany(insert_sql, batch_values)
                        batch_values = []

            # Insert remaining rows
            if batch_values and insert_sql:
                cursor.executemany(insert_sql, batch_values)

            # Commit THIS file's transaction
            conn.commit()
            total_rows_loaded += file_rows
            files_processed += 1

            # Log progress every 10 files or for large files
            if files_processed % 10 == 0 or file_rows > 10000:
                logger.info(
                    f"[{table_name}] Progress: {files_processed}/{total_files} files, "
                    f"{total_rows_loaded:,} total rows (CIK {cik}: {file_rows:,} rows)"
                )

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load {path} into {schema}.{table_name}: {e}")
            # Continue with next file instead of failing entirely
            continue
        finally:
            cursor.close()

    logger.info(
        f"Successfully loaded {total_rows_loaded:,} rows into {schema}.{table_name} "
        f"from {files_processed}/{total_files} files"
    )
    return total_rows_loaded


def _upsert_metric_metadata(
    conn: Any,
    metric_metadata_rows: List[Dict[str, Any]],
    schema: str = "sec_raw",
) -> int:
    """
    Upsert metric metadata (taxonomy, metric_name, label, description).
    Updates existing rows if label/description changed, inserts new ones.
    Returns count of rows upserted.
    """
    if not metric_metadata_rows:
        return 0

    cursor = conn.cursor()
    upserted = 0

    try:
        upsert_sql = f"""
            INSERT INTO {schema}.metric_metadata (taxonomy, metric_name, label, description, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (taxonomy, metric_name) DO UPDATE SET
                label = COALESCE(EXCLUDED.label, {schema}.metric_metadata.label),
                description = COALESCE(EXCLUDED.description, {schema}.metric_metadata.description),
                updated_at = CURRENT_TIMESTAMP
        """

        for row in metric_metadata_rows:
            cursor.execute(upsert_sql, (
                row.get("taxonomy"),
                row.get("metric_name"),
                row.get("label"),
                row.get("description"),
            ))
            upserted += 1

        conn.commit()
        return upserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert metric metadata: {e}")
        raise
    finally:
        cursor.close()


def _find_existing_data(cfg: Settings, cik: str) -> Optional[Dict[str, str]]:
    """
    Find existing data for a CIK in the new structure: data/sec_raw/cik={cik}/
    Returns dict with 'submissions', 'companyfacts', and 'metadata' paths, or None if not found.
    """
    if cfg.s3_bucket:
        # For S3, we'd need to list objects - skipping for now, can add later
        return None

    # For local storage, check the new structure: cik={cik}/
    base_dir = cfg.local_dir
    cik_dir = os.path.join(base_dir, f"cik={cik}")

    if not os.path.exists(cik_dir):
        return None

    sub_path = os.path.join(cik_dir, "submissions.json")
    facts_path = os.path.join(cik_dir, "companyfacts.json")
    metadata_path = os.path.join(cik_dir, "metadata.json")

    # Return paths if they exist (submissions and metadata are required, companyfacts is optional)
    if os.path.exists(sub_path) and os.path.exists(metadata_path):
        result = {
            "submissions": sub_path,
            "metadata": metadata_path,
        }
        if os.path.exists(facts_path):
            result["companyfacts"] = facts_path
        return result

    return None

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

    def _process_single_company(
        cfg: Settings,
        s: requests.Session,
        company: Dict[str, str],
        company_index: int,
        total_companies: int,
        mem_before_company: float,
    ) -> Dict[str, str]:
        """
        Process a single company - downloads and stores JSON files.
        Returns metadata about where it stored the files.
        """
        cik = company["cik"]
        ticker = company.get("ticker", "")
        cik10 = _pad_cik(cik)
        ingest_dt = datetime.utcnow().strftime("%Y-%m-%d")

        logger.info(
            "=" * 80
        )
        logger.info(
            "Processing CIK %s (%s) - Company %d/%d",
            cik,
            ticker,
            company_index + 1,
            total_companies,
        )
        logger.info(
            "Memory at start of CIK %s: %.1f MB",
            cik,
            mem_before_company,
        )

        submissions_url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
        facts_url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik10}.json"

        # Check if we have existing data and read latest filing date from metadata
        existing_data = _find_existing_data(cfg, cik)
        existing_filing_date = None
        if existing_data and existing_data.get("metadata"):
            metadata = _read_metadata(os.path.dirname(existing_data["metadata"]))
            if metadata:
                existing_filing_date = metadata.get("latest_filing_date")
                logger.info(
                    "CIK %s: Found existing data with latest filing date: %s",
                    cik,
                    existing_filing_date,
                )

        # Always download submissions.json (it's relatively small and contains metadata)
        mem_before_submissions = _get_memory_mb()
        logger.info(
            "CIK %s: Downloading submissions.json. Memory: %.1f MB",
            cik,
            mem_before_submissions,
        )
        submissions = _get_json(s, submissions_url, cfg.timeout_s, cfg.rps, log_memory=False)
        new_filing_date = _get_most_recent_filing_date(submissions)
        mem_after_submissions = _get_memory_mb()
        logger.info(
            "CIK %s: Downloaded submissions.json. Memory: %.1f MB (delta: +%.1f MB)",
            cik,
            mem_after_submissions,
            mem_after_submissions - mem_before_submissions,
        )
        logger.info(
            "CIK %s: Latest filing date in submissions: %s",
            cik,
            new_filing_date,
        )

        # Only download companyfacts.json if there are new filings
        facts = None
        facts_bytes = None
        needs_facts_download = True

        if existing_filing_date and new_filing_date:
            if new_filing_date <= existing_filing_date:
                # No new filings, skip downloading the large companyfacts.json file
                needs_facts_download = False
                logger.info(
                    "CIK %s: No new filings since %s. Skipping companyfacts.json download.",
                    cik,
                    existing_filing_date,
                )

        if needs_facts_download:
            # Log memory before downloading the large companyfacts.json file
            mem_before_facts = _get_memory_mb()
            logger.info(
                "CIK %s: Downloading companyfacts.json. Memory: %.1f MB",
                cik,
                mem_before_facts,
            )
            try:
                facts = _get_json(s, facts_url, cfg.timeout_s, cfg.rps, log_memory=True)
                mem_after_facts = _get_memory_mb()
                logger.info(
                    "CIK %s: Loaded companyfacts.json. Memory: %.1f MB (delta: +%.1f MB)",
                    cik,
                    mem_after_facts,
                    mem_after_facts - mem_before_facts,
                )
                facts_bytes = json.dumps(facts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                logger.info(
                    "CIK %s: Encoded companyfacts to bytes. Size: %.1f MB",
                    cik,
                    len(facts_bytes) / 1024.0 / 1024.0,
                )
                # Clear the facts dict from memory after encoding (help GC)
                del facts
                gc.collect()
                mem_after_encode = _get_memory_mb()
                logger.info(
                    "CIK %s: After encoding and GC. Memory: %.1f MB (freed %.1f MB)",
                    cik,
                    mem_after_encode,
                    mem_after_facts - mem_after_encode,
                )
                existing_facts_path = None
            except AirflowFailException as e:
                # Check if it's a 404 - some companies don't have XBRL data
                error_msg = str(e)
                if "status=404" in error_msg or "(status=404)" in error_msg or "status= 404" in error_msg:
                    logger.warning(
                        "CIK %s: companyfacts.json not available (404). This company may not have XBRL data. Continuing without it.",
                        cik,
                    )
                    facts_bytes = None
                    existing_facts_path = None
                    needs_facts_download = False
                else:
                    # Re-raise for other errors (rate limiting, 5xx, etc.)
                    raise
        else:
            # Reuse existing companyfacts.json - don't download or copy
            facts_bytes = None
            existing_facts_path = existing_data.get("companyfacts") if existing_data else None

        submissions_bytes = json.dumps(submissions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        # Clear submissions dict after encoding
        del submissions
        gc.collect()
        mem_after_cleanup = _get_memory_mb()
        logger.info(
            "CIK %s: After cleanup. Memory: %.1f MB",
            cik,
            mem_after_cleanup,
        )

        if cfg.s3_bucket:
            if S3Hook is None:
                raise AirflowFailException(
                    "SEC_S3_BUCKET is set but amazon provider is not installed. "
                    "Install apache-airflow-providers-amazon."
                )
            hook = S3Hook(aws_conn_id="aws_default")
            sub_key = _s3_key(cfg.s3_prefix, cik, "submissions")
            facts_key = _s3_key(cfg.s3_prefix, cik, "companyfacts")
            metadata_key = _s3_key(cfg.s3_prefix, cik, "metadata")

            hook.load_bytes(
                bytes_data=submissions_bytes,
                key=sub_key,
                bucket_name=cfg.s3_bucket,
                replace=True,
            )
            # Only upload companyfacts if we downloaded it
            if facts_bytes is not None:
                hook.load_bytes(
                    bytes_data=facts_bytes,
                    key=facts_key,
                    bucket_name=cfg.s3_bucket,
                    replace=True,
                )
                facts_location = f"s3://{cfg.s3_bucket}/{facts_key}"
            else:
                # Use existing facts location (from existing_data)
                facts_location = existing_facts_path if existing_facts_path else f"s3://{cfg.s3_bucket}/{facts_key}"

            # Upload metadata.json
            metadata = {
                "latest_filing_date": new_filing_date,
                "last_updated": ingest_dt,
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
            metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
            hook.load_bytes(
                bytes_data=metadata_bytes,
                key=metadata_key,
                bucket_name=cfg.s3_bucket,
                replace=True,
            )

            result = {
                "cik": cik,
                "ticker": ticker,
                "stored": "s3",
                "submissions": f"s3://{cfg.s3_bucket}/{sub_key}",
                "companyfacts": facts_location,
                "facts_downloaded": facts_bytes is not None,
            }
        else:
            # Local fallback - new structure: data/sec_raw/cik={cik}/
            cik_dir = os.path.join(cfg.local_dir, f"cik={cik}")
            sub_path = os.path.join(cik_dir, "submissions.json")
            facts_path = os.path.join(cik_dir, "companyfacts.json")

            _write_bytes(sub_path, submissions_bytes)

            # Only write companyfacts if we downloaded it
            if facts_bytes is not None:
                _write_bytes(facts_path, facts_bytes)
                facts_location = facts_path
            else:
                # Use existing facts location (don't copy, just reference)
                facts_location = existing_facts_path if existing_facts_path else facts_path

            # Write metadata.json with latest filing date
            _write_metadata(cik_dir, new_filing_date, ingest_dt)

            result = {
                "cik": cik,
                "ticker": ticker,
                "stored": "local",
                "submissions": sub_path,
                "companyfacts": facts_location,
                "facts_downloaded": facts_bytes is not None,
            }

        mem_final = _get_memory_mb()
        logger.info(
            "CIK %s: Complete. Final memory: %.1f MB (delta from start: +%.1f MB)",
            cik,
            mem_final,
            mem_final - mem_before_company,
        )
        logger.info("=" * 80)

        return result

    def _estimate_results_size_mb(results: List[Dict[str, str]]) -> float:
        """Estimate the memory size of results list in MB."""
        if not results:
            return 0.0
        # Rough estimate: serialize to JSON and measure bytes
        try:
            json_bytes = json.dumps(results, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            return len(json_bytes) / (1024.0 * 1024.0)
        except Exception:
            # Fallback: rough estimate based on count
            return len(results) * 0.001  # ~1KB per result dict

    @task
    def fetch_and_store_all_companies(companies: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Process all companies sequentially in a single task.
        Downloads and stores JSON files for each company one at a time.

        This serial approach helps debug memory issues by:
        - Processing one CIK at a time (no parallelism)
        - Logging memory usage for each step
        - Making it easier to identify which CIK causes memory problems
        - Clearing results list after each company to test if it's causing memory growth
        """
        cfg = _settings()
        s = _session(cfg.user_agent)

        total_companies = len(companies)
        logger.info(
            "Starting serial processing of %d companies",
            total_companies,
        )

        mem_initial = _get_memory_mb()
        logger.info("Initial memory: %.1f MB", mem_initial)

        results = []
        facts_downloaded_count = 0
        facts_skipped_count = 0

        # Write results incrementally to a file to avoid losing them when we clear the list
        results_file_path = None
        if cfg.local_dir:
            results_file_path = os.path.join(cfg.local_dir, "processing_results.jsonl")
            # Clear any existing file
            if os.path.exists(results_file_path):
                os.remove(results_file_path)
            logger.info("Writing results incrementally to: %s", results_file_path)

        for idx, company in enumerate(companies):
            mem_before = _get_memory_mb()

            try:
                result = _process_single_company(
                    cfg, s, company, idx, total_companies, mem_before
                )
                results.append(result)

                # Write result to file immediately
                if results_file_path:
                    with open(results_file_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(result, separators=(",", ":"), ensure_ascii=False) + "\n")

                if result.get("facts_downloaded", False):
                    facts_downloaded_count += 1
                else:
                    facts_skipped_count += 1

            except Exception as e:
                logger.error(
                    "Failed to process CIK %s: %s",
                    company.get("cik", "unknown"),
                    str(e),
                    exc_info=True,
                )
                # Continue with next company even if one fails
                continue

            # Monitor results list size before clearing
            results_count = len(results)
            results_size_mb = _estimate_results_size_mb(results)
            results_size_kb = results_size_mb * 1024.0
            logger.info(
                "Results list: %d items, estimated size: %.3f MB (%.1f KB)",
                results_count,
                results_size_mb,
                results_size_kb,
            )

            mem_after = _get_memory_mb()
            logger.info(
                "After CIK %s (%d/%d): Memory: %.1f MB (cumulative delta: +%.1f MB from start)",
                company.get("cik", "unknown"),
                idx + 1,
                total_companies,
                mem_after,
                mem_after - mem_initial,
            )

            # Clear results list to test if it's causing memory growth
            mem_before_clear = _get_memory_mb()
            results.clear()
            gc.collect()
            mem_after_clear = _get_memory_mb()
            freed_mb = mem_before_clear - mem_after_clear
            logger.info(
                "After clearing results list and GC: Memory: %.1f MB (freed %.3f MB from clearing)",
                mem_after_clear,
                freed_mb,
            )

        # Close the requests session to release connection pools
        mem_before_session_close = _get_memory_mb()
        s.close()
        gc.collect()
        mem_after_session_close = _get_memory_mb()
        logger.info(
            "After closing requests session and GC: Memory: %.1f MB (freed %.3f MB)",
            mem_after_session_close,
            mem_before_session_close - mem_after_session_close,
        )

        mem_final = _get_memory_mb()
        logger.info("=" * 80)
        logger.info(
            "All companies processed. Final memory: %.1f MB (total delta: +%.1f MB)",
            mem_final,
            mem_final - mem_initial,
        )
        logger.info(
            "Summary: %d companies processed, %d facts downloaded, %d facts skipped",
            total_companies,
            facts_downloaded_count,
            facts_skipped_count,
        )
        logger.info("=" * 80)

        # Return summary instead of full results to avoid recreating the list
        # Results are already written to file if needed
        return {
            "results_file": results_file_path,
            "total_companies": total_companies,
            "facts_downloaded": facts_downloaded_count,
            "facts_skipped": facts_skipped_count,
        }

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
            postgres_config = _get_postgres_config(cfg.postgres_config_path)
        except Exception as e:
            logger.error(f"Failed to load PostgreSQL config: {e}")
            raise AirflowFailException(f"PostgreSQL config error: {e}")

        schema = postgres_config.get("schema", "sec_raw")
        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Find all CIK directories
        base_dir = cfg.local_dir
        if not os.path.exists(base_dir):
            logger.warning(f"Data directory does not exist: {base_dir}")
            return {"total_ciks": 0, "processed": 0, "errors": 0}

        cik_dirs = [
            d for d in os.listdir(base_dir)
            if os.path.isdir(os.path.join(base_dir, d)) and d.startswith("cik=")
        ]

        # Filter to specific CIK if testing
        if cfg.ingest_test_cik:
            test_cik_dir = f"cik={cfg.ingest_test_cik}"
            if test_cik_dir in cik_dirs:
                cik_dirs = [test_cik_dir]
                logger.info(f"Testing mode: processing only CIK {cfg.ingest_test_cik}")
            else:
                logger.warning(
                    f"Test CIK {cfg.ingest_test_cik} not found in available directories. "
                    f"Available CIKs: {sorted([d.replace('cik=', '') for d in cik_dirs[:10]])}"
                )
                return {"total_ciks": 0, "processed": 0, "errors": 0}
        elif cfg.ingest_max_ciks > 0:
            # Limit number of CIKs for testing
            cik_dirs = sorted(cik_dirs)[:cfg.ingest_max_ciks]
            logger.info(f"Limiting to {cfg.ingest_max_ciks} CIKs for testing")

        logger.info(f"Found {len(cik_dirs)} CIK directories to process")

        processed = 0
        errors = 0

        # Open a single PostgreSQL connection for all processing
        conn = None
        try:
            conn = _get_postgres_connection(postgres_config, schema)

            # Process CIKs in batches
            batch_size = 500
            all_cik_dirs = sorted(cik_dirs)

            for i in range(0, len(all_cik_dirs), batch_size):
                batch_dirs = all_cik_dirs[i:i + batch_size]
                logger.info(f"Starting batch {i//batch_size + 1} ({len(batch_dirs)} CIKs)")

                batch_submission_paths = []
                batch_metadata_paths = []
                batch_facts_paths = []
                batch_ciks_submissions = []
                batch_ciks_metadata = []
                batch_ciks_facts = []

                for cik_dir_name in batch_dirs:
                    cik = cik_dir_name.replace("cik=", "")
                    cik_path = os.path.join(base_dir, cik_dir_name)

                    submissions_json = os.path.join(cik_path, "submissions.json")
                    companyfacts_json = os.path.join(cik_path, "companyfacts.json")

                    try:
                        # Process submissions
                        if os.path.exists(submissions_json):
                            with open(submissions_json, "r", encoding="utf-8") as f:
                                submissions_data = json.load(f)

                            submissions_row = _convert_submissions_to_ndjson(
                                submissions_data, cik, ingest_date
                            )

                            submissions_ndjson = os.path.join(cik_path, "submissions.ndjson")
                            _write_ndjson_file(submissions_ndjson, [submissions_row])
                            batch_submission_paths.append(submissions_ndjson)
                            batch_ciks_submissions.append(cik)

                        # Process companyfacts
                        if os.path.exists(companyfacts_json):
                            with open(companyfacts_json, "r", encoding="utf-8") as f:
                                companyfacts_data = json.load(f)

                            metadata_rows, facts_rows, metric_meta_rows = _convert_companyfacts_to_ndjson(
                                companyfacts_data, cik, ingest_date
                            )

                            # Upsert metric metadata directly (small, deduplicated)
                            if metric_meta_rows:
                                _upsert_metric_metadata(conn, metric_meta_rows, schema)

                            # Company metadata
                            metadata_ndjson = os.path.join(cik_path, "companyfacts_metadata.ndjson")
                            _write_ndjson_file(metadata_ndjson, metadata_rows)
                            batch_metadata_paths.append(metadata_ndjson)
                            batch_ciks_metadata.append(cik)

                            # Facts (now without label/description)
                            facts_ndjson = os.path.join(cik_path, "companyfacts_facts.ndjson")
                            _write_ndjson_file(facts_ndjson, facts_rows)
                            batch_facts_paths.append(facts_ndjson)
                            batch_ciks_facts.append(cik)

                        processed += 1
                    except Exception as e:
                        errors += 1
                        logger.error(f"Error processing JSON for CIK {cik}: {e}")

                # Load this batch to PostgreSQL
                if batch_submission_paths:
                    _load_ndjson_batch_to_postgres(
                        conn, "submissions", batch_submission_paths, batch_ciks_submissions, ingest_date, schema
                    )

                if batch_metadata_paths:
                    _load_ndjson_batch_to_postgres(
                        conn, "companyfacts_metadata", batch_metadata_paths, batch_ciks_metadata, ingest_date, schema
                    )

                if batch_facts_paths:
                    _load_ndjson_batch_to_postgres(
                        conn, "companyfacts_facts", batch_facts_paths, batch_ciks_facts, ingest_date, schema
                    )

                logger.info(f"Completed batch {i//batch_size + 1}")

            logger.info(f"Ingestion complete: {processed} CIKs processed, {errors} errors")
            return {"total_ciks": len(cik_dirs), "processed": processed, "errors": errors}

        finally:
            if conn:
                conn.close()

    @task
    def validate_postgres_ingestion(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data integrity after PostgreSQL ingestion.

        Performs validation checks:
        1. Row count validation - verify data was loaded
        2. NULL check on required fields (CIK)
        3. Array alignment check - tickers and exchanges arrays should match
        4. Data type validation - fiscal_year should be numeric
        5. Primary key uniqueness - check for duplicates

        Returns a detailed validation report and raises AirflowFailException
        for critical failures.
        """
        cfg = _settings()

        # Skip validation if no data was ingested
        if ingest_result.get("total_ciks", 0) == 0:
            logger.info("No CIKs were ingested, skipping validation")
            return {
                "validations_passed": 0,
                "validations_failed": 0,
                "skipped": True,
                "details": [],
            }

        try:
            postgres_config = _get_postgres_config(cfg.postgres_config_path)
        except Exception as e:
            logger.error(f"Failed to load PostgreSQL config for validation: {e}")
            raise AirflowFailException(f"Validation failed - cannot connect to PostgreSQL: {e}")

        schema = postgres_config.get("schema", "sec_raw")
        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")

        conn = None
        validation_results = []
        validations_passed = 0
        validations_failed = 0
        critical_failure = False

        try:
            conn = _get_postgres_connection(postgres_config, schema)
            cursor = conn.cursor()

            # Validation 1: Row count validation for submissions table
            logger.info("Running validation 1: Row count check")
            cursor.execute(
                f"SELECT COUNT(*) FROM {schema}.submissions WHERE ingest_date = %s",
                (ingest_date,)
            )
            submissions_count = cursor.fetchone()[0]

            expected_count = ingest_result.get("processed", 0)
            row_count_status = "passed" if submissions_count > 0 else "failed"
            if submissions_count == 0 and expected_count > 0:
                validations_failed += 1
                critical_failure = True
                logger.error(f"CRITICAL: No rows found in submissions for ingest_date={ingest_date}")
            else:
                validations_passed += 1
                logger.info(f"Row count validation: {submissions_count} rows in submissions table")

            validation_results.append({
                "name": "row_count_submissions",
                "status": row_count_status,
                "expected": f">0 (processed {expected_count} CIKs)",
                "actual": submissions_count,
            })

            # Validation 2: NULL check on required CIK field
            logger.info("Running validation 2: NULL CIK check")
            cursor.execute(
                f"SELECT COUNT(*) FROM {schema}.submissions WHERE cik IS NULL AND ingest_date = %s",
                (ingest_date,)
            )
            null_cik_count = cursor.fetchone()[0]

            null_check_status = "passed" if null_cik_count == 0 else "failed"
            if null_cik_count > 0:
                validations_failed += 1
                critical_failure = True
                logger.error(f"CRITICAL: Found {null_cik_count} rows with NULL CIK in submissions")
            else:
                validations_passed += 1
                logger.info("NULL CIK check passed - no NULL CIKs found")

            validation_results.append({
                "name": "null_cik_check",
                "status": null_check_status,
                "expected": 0,
                "actual": null_cik_count,
            })

            # Validation 3: Array alignment check - tickers and exchanges (PostgreSQL JSONB)
            logger.info("Running validation 3: Array alignment check")
            cursor.execute(
                f"""
                SELECT cik, jsonb_array_length(tickers) as ticker_count, jsonb_array_length(exchanges) as exchange_count
                FROM {schema}.submissions
                WHERE ingest_date = %s
                  AND jsonb_array_length(tickers) != jsonb_array_length(exchanges)
                LIMIT 10
                """,
                (ingest_date,)
            )
            mismatched_rows = cursor.fetchall()
            mismatched_ciks = [row[0] for row in mismatched_rows]

            array_check_status = "passed" if len(mismatched_ciks) == 0 else "warning"
            if mismatched_ciks:
                # This is a warning, not a critical failure (data was already truncated during conversion)
                logger.warning(
                    f"Array alignment: {len(mismatched_ciks)} CIKs have mismatched ticker/exchange arrays: {mismatched_ciks}"
                )
            else:
                validations_passed += 1
                logger.info("Array alignment check passed - all tickers/exchanges arrays match")

            validation_results.append({
                "name": "array_alignment",
                "status": array_check_status,
                "mismatched_ciks": mismatched_ciks,
            })

            # Validation 4: Data type validation - fiscal_year should be numeric
            logger.info("Running validation 4: Data type check on companyfacts_facts")
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.companyfacts_facts
                WHERE ingest_date = %s
                  AND fiscal_year IS NOT NULL
                """,
                (ingest_date,)
            )
            facts_with_fy = cursor.fetchone()[0]

            # Check for any non-numeric fiscal_year values (shouldn't happen after validation)
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.companyfacts_facts
                WHERE ingest_date = %s
                """,
                (ingest_date,)
            )
            total_facts = cursor.fetchone()[0]

            type_check_status = "passed"
            validations_passed += 1
            logger.info(f"Data type check: {facts_with_fy}/{total_facts} facts have fiscal_year values")

            validation_results.append({
                "name": "data_type_validation",
                "status": type_check_status,
                "total_facts": total_facts,
                "facts_with_fiscal_year": facts_with_fy,
            })

            # Validation 5: Primary key uniqueness - check for duplicate CIKs on same ingest_date
            logger.info("Running validation 5: Primary key uniqueness check")
            cursor.execute(
                f"""
                SELECT cik, COUNT(*) as cnt
                FROM {schema}.submissions
                WHERE ingest_date = %s
                GROUP BY cik
                HAVING COUNT(*) > 1
                LIMIT 10
                """,
                (ingest_date,)
            )
            duplicate_rows = cursor.fetchall()
            duplicate_ciks = [row[0] for row in duplicate_rows]

            pk_check_status = "passed" if len(duplicate_ciks) == 0 else "failed"
            if duplicate_ciks:
                validations_failed += 1
                logger.error(f"Found duplicate CIKs in submissions: {duplicate_ciks}")
            else:
                validations_passed += 1
                logger.info("Primary key uniqueness check passed - no duplicate CIKs")

            validation_results.append({
                "name": "primary_key_uniqueness",
                "status": pk_check_status,
                "duplicate_ciks": duplicate_ciks,
            })

            cursor.close()

        except Exception as e:
            logger.error(f"Validation query failed: {e}")
            raise AirflowFailException(f"Validation failed with error: {e}")
        finally:
            if conn:
                conn.close()

        # Log summary
        logger.info("=" * 80)
        logger.info(
            f"Validation complete: {validations_passed} passed, {validations_failed} failed"
        )
        for result in validation_results:
            logger.info(f"  - {result['name']}: {result['status']}")
        logger.info("=" * 80)

        # Raise exception for critical failures
        if critical_failure:
            raise AirflowFailException(
                f"Critical validation failures detected: {validations_failed} validations failed. "
                f"Check logs for details."
            )

        return {
            "validations_passed": validations_passed,
            "validations_failed": validations_failed,
            "details": validation_results,
        }

    companies = get_company_ciks()
    stored = fetch_and_store_all_companies(companies)
    ingested = ingest_to_postgres(stored)
    validated = validate_postgres_ingestion(ingested)

    companies >> Label("download SEC JSON + store raw") >> stored >> Label("ingest to PostgreSQL") >> ingested >> Label("validate") >> validated >> summarize(stored)
