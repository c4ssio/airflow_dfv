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

        Returns metadata about where it stored the files.
        """
        cfg = _settings()
        s = _session(cfg.user_agent)

        cik = company["cik"]
        cik10 = _pad_cik(cik)
        ingest_dt = datetime.utcnow().strftime("%Y-%m-%d")

        submissions_url = f"{SEC_BASE}/submissions/CIK{cik10}.json"
        facts_url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik10}.json"

        submissions = _get_json(s, submissions_url, cfg.timeout_s, cfg.rps)
        facts = _get_json(s, facts_url, cfg.timeout_s, cfg.rps)

        submissions_bytes = json.dumps(submissions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        facts_bytes = json.dumps(facts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

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
            hook.load_bytes(
                bytes_data=facts_bytes,
                key=facts_key,
                bucket_name=cfg.s3_bucket,
                replace=True,
            )
            return {
                "cik": cik,
                "ticker": company.get("ticker", ""),
                "stored": "s3",
                "submissions": f"s3://{cfg.s3_bucket}/{sub_key}",
                "companyfacts": f"s3://{cfg.s3_bucket}/{facts_key}",
            }

        # Local fallback
        base = os.path.join(cfg.local_dir, f"ingest_date={ingest_dt}", f"cik={cik}")
        sub_path = os.path.join(base, "submissions.json")
        facts_path = os.path.join(base, "companyfacts.json")
        _write_bytes(sub_path, submissions_bytes)
        _write_bytes(facts_path, facts_bytes)
        return {
            "cik": cik,
            "ticker": company.get("ticker", ""),
            "stored": "local",
            "submissions": sub_path,
            "companyfacts": facts_path,
        }

    @task
    def summarize(results: List[Dict[str, str]]) -> None:
        stored_s3 = sum(1 for r in results if r.get("stored") == "s3")
        stored_local = sum(1 for r in results if r.get("stored") == "local")
        print(f"Done. Stored to S3: {stored_s3}, stored locally: {stored_local}")
        if results:
            print("Sample output:")
            print(json.dumps(results[0], indent=2))

    companies = get_company_ciks()
    stored = fetch_and_store_company_json.expand(company=companies)

    companies >> Label("download SEC JSON + store raw") >> stored >> summarize(stored)
