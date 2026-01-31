from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

from scripts.sec_scraper.common import Settings


def s3_key(prefix: str, cik: str, name: str) -> str:
    return f"{prefix}/cik={cik}/{name}.json"


def write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def get_most_recent_filing_date(submissions_data: Dict[str, Any]) -> Optional[str]:
    """Extract the most recent filing date from submissions.json data."""
    filings = submissions_data.get("filings")
    if not filings:
        return None
    recent = filings.get("recent")
    if not recent:
        return None
    filing_dates = recent.get("filingDate")
    if not filing_dates:
        return None
    return max(filing_dates)


def read_metadata(cik_dir: str) -> Optional[Dict[str, Any]]:
    """Read metadata.json from a CIK directory."""
    metadata_path = os.path.join(cik_dir, "metadata.json")
    if not os.path.exists(metadata_path):
        return None
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_metadata(cik_dir: str, latest_filing_date: Optional[str], ingest_date: str) -> None:
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


def find_existing_data(cfg: Settings, cik: str) -> Optional[Dict[str, str]]:
    """Find existing data for a CIK in the new structure: data/sec_raw/cik={cik}/"""
    if cfg.s3_bucket:
        # For S3, we'd need to list objects - skipping for now, can add later
        return None

    base_dir = cfg.local_dir
    cik_dir = os.path.join(base_dir, f"cik={cik}")

    if not os.path.exists(cik_dir):
        return None

    sub_path = os.path.join(cik_dir, "submissions.json")
    facts_path = os.path.join(cik_dir, "companyfacts.json")
    metadata_path = os.path.join(cik_dir, "metadata.json")

    if os.path.exists(sub_path) and os.path.exists(metadata_path):
        result = {
            "submissions": sub_path,
            "metadata": metadata_path,
        }
        if os.path.exists(facts_path):
            result["companyfacts"] = facts_path
        return result

    return None
