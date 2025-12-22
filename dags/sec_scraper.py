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

import json
import logging
import os
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


# --- Simple global rate limiter (per task instance) ---
_last_request_ts: float = 0.0


def _rate_limit(rps: float) -> None:
    """Sleep so we don't exceed rps within a single task instance."""
    global _last_request_ts
    if rps <= 0:
        return
    min_interval = 1.0 / rps
    now = time.time()
    wait = (_last_request_ts + min_interval) - now
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.time()


def _get_json(
    s: requests.Session,
    url: str,
    timeout_s: int,
    rps: float,
    max_attempts: int = 5,
) -> Dict[str, Any]:
    """
    Robust GET with backoff on 429/5xx.
    """
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        _rate_limit(rps)
        resp = s.get(url, timeout=timeout_s)
        if resp.status_code == 200:
            return resp.json()

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


def _s3_key(prefix: str, ingest_dt: str, cik: str, name: str) -> str:
    return f"{prefix}/ingest_date={ingest_dt}/cik={cik}/{name}.json"


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


def _find_existing_data(cfg: Settings, cik: str) -> Optional[Dict[str, str]]:
    """
    Find the most recent existing data for a CIK.
    Returns dict with 'submissions' and 'companyfacts' paths, or None if not found.
    """
    if cfg.s3_bucket:
        # For S3, we'd need to list objects - skipping for now, can add later
        return None

    # For local storage, find the most recent ingest_date directory
    base_dir = cfg.local_dir
    if not os.path.exists(base_dir):
        return None

    # Find all ingest_date directories
    ingest_dirs = []
    for item in os.listdir(base_dir):
        if item.startswith("ingest_date="):
            cik_dir = os.path.join(base_dir, item, f"cik={cik}")
            sub_path = os.path.join(cik_dir, "submissions.json")
            facts_path = os.path.join(cik_dir, "companyfacts.json")
            if os.path.exists(sub_path) and os.path.exists(facts_path):
                ingest_dirs.append((item, sub_path, facts_path))

    if not ingest_dirs:
        return None

    # Sort by ingest_date (descending) and return most recent
    ingest_dirs.sort(key=lambda x: x[0], reverse=True)
    _, sub_path, facts_path = ingest_dirs[0]
    return {"submissions": sub_path, "companyfacts": facts_path}


def _read_existing_submissions(path: str) -> Optional[Dict[str, Any]]:
    """Read existing submissions.json file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
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

    @task
    def fetch_and_store_company_json(company: Dict[str, str]) -> Dict[str, str]:
        """
        For one company, download:
          - /submissions/CIK##########.json
          - /api/xbrl/companyfacts/CIK##########.json

        Store raw JSON in S3 (if configured) else local dir.

        Implements incremental updates: only downloads companyfacts.json (the large file)
        if there are new filings since the last run.

        Returns metadata about where it stored the files.
        """
        cfg = _settings()
        s = _session(cfg.user_agent)

        cik = company["cik"]
        cik10 = _pad_cik(cik)
        ingest_dt = datetime.utcnow().strftime("%Y-%m-%d")

        submissions_url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
        facts_url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik10}.json"

        # Check if we have existing data
        existing_data = _find_existing_data(cfg, cik)
        existing_filing_date = None
        if existing_data and existing_data.get("submissions"):
            existing_submissions = _read_existing_submissions(existing_data["submissions"])
            if existing_submissions:
                existing_filing_date = _get_most_recent_filing_date(existing_submissions)

        # Always download submissions.json (it's relatively small and contains metadata)
        submissions = _get_json(s, submissions_url, cfg.timeout_s, cfg.rps)
        new_filing_date = _get_most_recent_filing_date(submissions)

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
            facts = _get_json(s, facts_url, cfg.timeout_s, cfg.rps)
            facts_bytes = json.dumps(facts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            existing_facts_path = None
        else:
            # Reuse existing companyfacts.json - don't download or copy
            facts_bytes = None
            existing_facts_path = existing_data.get("companyfacts") if existing_data else None

        submissions_bytes = json.dumps(submissions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        if cfg.s3_bucket:
            if S3Hook is None:
                raise AirflowFailException(
                    "SEC_S3_BUCKET is set but amazon provider is not installed. "
                    "Install apache-airflow-providers-amazon."
                )
            hook = S3Hook(aws_conn_id="aws_default")
            sub_key = _s3_key(cfg.s3_prefix, ingest_dt, cik, "submissions")
            facts_key = _s3_key(cfg.s3_prefix, ingest_dt, cik, "companyfacts")

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

            return {
                "cik": cik,
                "ticker": company.get("ticker", ""),
                "stored": "s3",
                "submissions": f"s3://{cfg.s3_bucket}/{sub_key}",
                "companyfacts": facts_location,
                "facts_downloaded": facts_bytes is not None,
            }

        # Local fallback
        base = os.path.join(cfg.local_dir, f"ingest_date={ingest_dt}", f"cik={cik}")
        sub_path = os.path.join(base, "submissions.json")
        facts_path = os.path.join(base, "companyfacts.json")
        _write_bytes(sub_path, submissions_bytes)

        # Only write companyfacts if we downloaded it
        if facts_bytes is not None:
            _write_bytes(facts_path, facts_bytes)
            facts_location = facts_path
        else:
            # Use existing facts location (don't copy, just reference)
            facts_location = existing_facts_path if existing_facts_path else facts_path

        return {
            "cik": cik,
            "ticker": company.get("ticker", ""),
            "stored": "local",
            "submissions": sub_path,
            "companyfacts": facts_location,
            "facts_downloaded": facts_bytes is not None,
        }

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
    stored = fetch_and_store_company_json.expand(company=companies)

    companies >> Label("download SEC JSON + store raw") >> stored >> summarize(stored)
