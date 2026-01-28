from __future__ import annotations

import logging
import os
import resource
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

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

