from __future__ import annotations

import logging
import os
import resource
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class SecScraperConfigError(RuntimeError):
    """Configuration error for the SEC scraper."""


@dataclass(frozen=True)
class Settings:
    user_agent: str
    rps: float
    timeout_s: int
    max_ciks: int
    start_cik: str
    s3_bucket: str
    s3_prefix: str
    local_dir: str
    postgres_config_path: str
    ingest_test_cik: str
    ingest_max_ciks: int


def load_settings(dag_file: str) -> Settings:
    """Load settings from environment variables.

    `dag_file` is used only to compute a sensible default for local development.
    """
    user_agent = os.environ.get("SEC_USER_AGENT", "").strip()
    if not user_agent:
        raise SecScraperConfigError(
            "SEC_USER_AGENT is required. Set it to something like: "
            "'drclive SEC scraper (you@drclive.net)'"
        )

    rps = float(os.environ.get("SEC_REQUESTS_PER_SECOND", "5"))
    timeout_s = int(os.environ.get("SEC_TIMEOUT_SECONDS", "30"))
    max_ciks = int(os.environ.get("SEC_MAX_CIKS_PER_RUN", "250"))
    start_cik = os.environ.get("SEC_START_CIK", "").strip()

    s3_bucket = os.environ.get("SEC_S3_BUCKET", "").strip()
    s3_prefix = os.environ.get("SEC_S3_PREFIX", "sec_raw").strip().strip("/")

    local_dir = os.environ.get("SEC_LOCAL_DIR", "/tmp/sec_raw").strip()

    # Default path: /opt/airflow/config/postgres.yaml (when running in container)
    # or ./config/postgres.yaml (when running locally)
    default_config_path = "/opt/airflow/config/postgres.yaml"
    if not os.path.exists(default_config_path):
        # Fallback for local development
        default_config_path = str(Path(dag_file).resolve().parent.parent / "config" / "postgres.yaml")

    postgres_config_path = os.environ.get("POSTGRES_CONFIG_PATH", default_config_path).strip()

    ingest_test_cik = os.environ.get("SEC_INGEST_TEST_CIK", "").strip()
    ingest_max_ciks = int(os.environ.get("SEC_INGEST_MAX_CIKS", "0"))  # 0 means no limit

    return Settings(
        user_agent=user_agent,
        rps=rps,
        timeout_s=timeout_s,
        max_ciks=max_ciks,
        start_cik=start_cik,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        local_dir=local_dir,
        postgres_config_path=postgres_config_path,
        ingest_test_cik=ingest_test_cik,
        ingest_max_ciks=ingest_max_ciks,
    )


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
    )
    return s


# --- Shared rate limiter for coordinated rate limiting across tasks ---
_rate_limit_lock = threading.Lock()
_last_request_ts: float = 0.0


def rate_limit(rps: float) -> None:
    """Sleep so we don't exceed rps within a single worker process."""
    global _last_request_ts

    if rps <= 0:
        return

    with _rate_limit_lock:
        min_interval = 1.0 / rps
        now = time.time()
        wait = (_last_request_ts + min_interval) - now
        if wait <= 0:
            wait = 0

    if wait > 0:
        time.sleep(wait)

    with _rate_limit_lock:
        _last_request_ts = time.time()


def get_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:
        return 0.0


def get_json(
    s: requests.Session,
    url: str,
    timeout_s: int,
    rps: float,
    max_attempts: int = 5,
    log_memory: bool = False,
) -> Dict[str, Any]:
    """Robust GET with backoff on 429/5xx."""
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        rate_limit(rps)
        mem_before = get_memory_mb() if log_memory else 0.0

        resp = s.get(url, timeout=timeout_s)
        if resp.status_code == 200:
            if log_memory:
                logger.info(
                    "Memory before JSON parse (%s): %.1f MB, Response size: %.1f MB",
                    url,
                    mem_before,
                    len(resp.content) / 1024.0 / 1024.0,
                )
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning(
                "SEC request failed (attempt %d/%d) url=%s status=%s, sleeping %.1fs",
                attempt,
                max_attempts,
                url,
                resp.status_code,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            continue

        raise RuntimeError(f"SEC request failed: url={url} status={resp.status_code} body={resp.text[:500]}")

    raise RuntimeError(f"SEC request failed after {max_attempts} attempts: {url}")


# -------------------------
# Data normalization helpers
# -------------------------

def pad_cik(cik: str) -> str:
    """Zero-pad a CIK to 10 digits (SEC convention)."""
    return cik.zfill(10)


def validate_date_string(value: Any, field_name: str, cik: str) -> Optional[str]:
    """Validate that a value is a valid date string (YYYY-MM-DD-ish) or None."""
    if value is None:
        return None
    if not isinstance(value, str):
        logger.warning("CIK %s: %s has non-string value: %s", cik, field_name, type(value).__name__)
        return str(value)
    if value and len(value) >= 10:
        try:
            parts = value[:10].split("-")
            if len(parts) == 3:
                int(parts[0])
                int(parts[1])
                int(parts[2])
        except (ValueError, IndexError):
            logger.warning("CIK %s: %s has invalid date format: %s", cik, field_name, value)
    return value


def validate_numeric(value: Any, field_name: str, cik: str) -> Any:
    """Validate that a value is numeric or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            logger.warning("CIK %s: %s has non-numeric string value: %s", cik, field_name, value)
            return None
    logger.warning("CIK %s: %s has unexpected type: %s", cik, field_name, type(value).__name__)
    return None


def validate_integer(value: Any, field_name: str, cik: str) -> Optional[int]:
    """Validate that a value is an integer or None."""
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value == int(value):
            return int(value)
        logger.warning("CIK %s: %s has non-integer float value: %s", cik, field_name, value)
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            logger.warning("CIK %s: %s has non-integer string value: %s", cik, field_name, value)
            return None
    logger.warning("CIK %s: %s has unexpected type: %s", cik, field_name, type(value).__name__)
    return None


def convert_submissions_to_ndjson(
    submissions_data: Dict[str, Any], cik: str, ingest_date: str
) -> Dict[str, Any]:
    """Convert submissions.json data to a single NDJSON-ready row."""
    cik_padded = pad_cik(cik)

    tickers = submissions_data.get("tickers", [])
    exchanges = submissions_data.get("exchanges", [])

    if len(tickers) != len(exchanges):
        logger.warning(
            "CIK %s: tickers (%d) and exchanges (%d) lengths differ. Truncating.",
            cik_padded,
            len(tickers),
            len(exchanges),
        )
        min_len = min(len(tickers), len(exchanges))
        tickers = tickers[:min_len]
        exchanges = exchanges[:min_len]

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


def convert_companyfacts_to_ndjson(
    companyfacts_data: Dict[str, Any], cik: str, ingest_date: str
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Convert companyfacts.json to (metadata_rows, facts_rows, metric_metadata_rows)."""
    cik_padded = pad_cik(cik)

    metadata_row = {
        "cik": cik_padded,
        "entity_name": companyfacts_data.get("entityName"),
        "ingest_date": ingest_date,
    }

    facts_rows: List[Dict[str, Any]] = []
    metric_metadata: Dict[tuple[str, str], Dict[str, Any]] = {}
    facts_obj = companyfacts_data.get("facts", {})
    type_warnings = 0
    max_type_warnings = 10

    for taxonomy in ["dei", "us-gaap"]:
        taxonomy_facts = facts_obj.get(taxonomy, {})
        if not isinstance(taxonomy_facts, dict):
            continue

        for metric_name, metric_data in taxonomy_facts.items():
            if not isinstance(metric_data, Dict):
                continue

            label = metric_data.get("label")
            description = metric_data.get("description")
            units = metric_data.get("units", {})

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

                    validated_fy = validate_integer(fiscal_year, "fiscal_year", cik_padded)
                    if fiscal_year is not None and validated_fy is None and type_warnings < max_type_warnings:
                        type_warnings += 1

                    validated_value = validate_numeric(value, "value", cik_padded)
                    if value is not None and validated_value is None and type_warnings < max_type_warnings:
                        type_warnings += 1

                    validated_period_end = validate_date_string(period_end, "period_end", cik_padded)
                    validated_filed_date = validate_date_string(filed_date, "filed_date", cik_padded)

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
        logger.warning(
            "CIK %s: Suppressed additional type validation warnings (total: %d+)",
            cik_padded,
            type_warnings,
        )

    metric_metadata_rows = list(metric_metadata.values())
    return [metadata_row], facts_rows, metric_metadata_rows


