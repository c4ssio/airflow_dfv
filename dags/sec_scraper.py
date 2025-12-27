"""
sec_scraper.py

Airflow DAG: Download SEC EDGAR data (company submissions + company facts)
for a batch of companies and store raw JSON in S3 (preferred) or local disk.

Environment variables (optional):
- SEC_USER_AGENT: REQUIRED by SEC guidance; include a real email.
  Example: "drclive SEC scraper (you@drclive.net)"
- SEC_REQUESTS_PER_SECOND: default 5  (keep <= 10; be polite)
- SEC_TIMEOUT_SECONDS: default 30
- SEC_MAX_CIKS_PER_RUN: default 50
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
from typing import Any, Dict, List, Optional

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.utils.edgemodifier import Label

logger = logging.getLogger(__name__)

try:
    # Only needed if you set SEC_S3_BUCKET
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
except Exception:  # pragma: no cover
    S3Hook = None  # type: ignore

SEC_BASE = "https://data.sec.gov"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

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

def _settings() -> Settings:
    user_agent = os.environ.get("SEC_USER_AGENT", "").strip()
    if not user_agent:
        raise AirflowFailException(
            "SEC_USER_AGENT is required. Set it to something like: "
            "'drclive SEC scraper (you@drclive.net)'"
        )

    rps = float(os.environ.get("SEC_REQUESTS_PER_SECOND", "5"))
    timeout_s = int(os.environ.get("SEC_TIMEOUT_SECONDS", "30"))
    max_ciks = int(os.environ.get("SEC_MAX_CIKS_PER_RUN", "50"))
    start_cik = os.environ.get("SEC_START_CIK", "").strip()

    s3_bucket = os.environ.get("SEC_S3_BUCKET", "").strip()
    s3_prefix = os.environ.get("SEC_S3_PREFIX", "sec_raw").strip().strip("/")

    local_dir = os.environ.get("SEC_LOCAL_DIR", "/tmp/sec_raw").strip()

    return Settings(
        user_agent=user_agent,
        rps=rps,
        timeout_s=timeout_s,
        max_ciks=max_ciks,
        start_cik=start_cik,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        local_dir=local_dir,
    )

def _session(user_agent: str) -> requests.Session:
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
        data = _get_json(s, TICKERS_URL, cfg.timeout_s, cfg.rps)

        # company_tickers.json is a dict keyed by integers as strings.
        # Each value has fields like cik_str, ticker, title.
        rows: List[Dict[str, str]] = []
        for _, v in data.items():
            cik = str(v.get("cik_str", "")).strip()
            ticker = str(v.get("ticker", "")).strip()
            title = str(v.get("title", "")).strip()
            if cik and ticker:
                rows.append({"cik": cik, "ticker": ticker, "title": title})

        # Stable order: by cik
        rows.sort(key=lambda r: int(r["cik"]))

        # Optional: start from a particular CIK
        if cfg.start_cik:
            rows = [r for r in rows if int(r["cik"]) >= int(cfg.start_cik)]

        # Limit per run for sanity
        return rows[: cfg.max_ciks]

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

    @task
    def fetch_and_store_all_companies(companies: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Process all companies sequentially in a single task.
        Downloads and stores JSON files for each company one at a time.

        This serial approach helps debug memory issues by:
        - Processing one CIK at a time (no parallelism)
        - Logging memory usage for each step
        - Making it easier to identify which CIK causes memory problems
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

        for idx, company in enumerate(companies):
            mem_before = _get_memory_mb()

            try:
                result = _process_single_company(
                    cfg, s, company, idx, total_companies, mem_before
                )
                results.append(result)

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

            mem_after = _get_memory_mb()
            logger.info(
                "After CIK %s (%d/%d): Memory: %.1f MB (cumulative delta: +%.1f MB from start)",
                company.get("cik", "unknown"),
                idx + 1,
                total_companies,
                mem_after,
                mem_after - mem_initial,
            )

            # Force garbage collection between companies to help identify leaks
            gc.collect()
            mem_after_gc = _get_memory_mb()
            if mem_after_gc < mem_after:
                logger.info(
                    "After GC: Memory: %.1f MB (freed %.1f MB)",
                    mem_after_gc,
                    mem_after - mem_after_gc,
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
            len(results),
            facts_downloaded_count,
            facts_skipped_count,
        )
        logger.info("=" * 80)

        return results

    @task
    def summarize(results: List[Dict[str, str]]) -> None:
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

    companies = get_company_ciks()
    stored = fetch_and_store_all_companies(companies)

    companies >> Label("download SEC JSON + store raw") >> stored >> summarize(stored)
