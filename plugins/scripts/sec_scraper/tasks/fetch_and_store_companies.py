from __future__ import annotations

import gc
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import requests

from airflow.exceptions import AirflowFailException

from scripts.sec_scraper.common import Settings, get_json, get_memory_mb, pad_cik
from scripts.sec_scraper.storage import (
    find_existing_data,
    read_metadata,
    write_bytes,
    write_metadata,
)

logger = logging.getLogger(__name__)


def process_single_company(
    cfg: Settings,
    session: requests.Session,
    company: Dict[str, str],
    *,
    sec_base: str,
    find_existing_data_fn=None,
    read_metadata_fn=None,
    write_bytes_fn=None,
    write_metadata_fn=None,
) -> Dict[str, str]:
    """Core logic for processing a single company (download + store JSON)."""
    if find_existing_data_fn is None or read_metadata_fn is None:
        raise RuntimeError("find_existing_data_fn and read_metadata_fn must be provided")
    if write_bytes_fn is None or write_metadata_fn is None:
        raise RuntimeError("write_bytes_fn and write_metadata_fn must be provided")

    cik = company["cik"]
    ticker = company.get("ticker", "")
    cik10 = pad_cik(cik)
    ingest_dt = datetime.utcnow().strftime("%Y-%m-%d")

    submissions_url = f"{sec_base}/submissions/CIK{cik10}.json"
    facts_url = f"{sec_base}/api/xbrl/companyfacts/CIK{cik10}.json"

    existing_data = find_existing_data_fn(cfg, cik)
    existing_filing_date = None
    if existing_data and existing_data.get("metadata"):
        metadata = read_metadata_fn(os.path.dirname(existing_data["metadata"]))
        if metadata:
            existing_filing_date = metadata.get("latest_filing_date")

    submissions = get_json(session, submissions_url, cfg.timeout_s, cfg.rps, log_memory=False)

    # Extract most recent filing date
    def _most_recent_filing_date(sub_data: Dict[str, Any]) -> str | None:
        filings = sub_data.get("filings") or {}
        recent = filings.get("recent") or {}
        filing_dates = recent.get("filingDate") or []
        if not filing_dates:
            return None
        return max(filing_dates)

    new_filing_date = _most_recent_filing_date(submissions)

    # Only download companyfacts.json if there are new filings
    facts = None
    facts_bytes = None
    needs_facts_download = True

    if existing_filing_date and new_filing_date:
        if new_filing_date <= existing_filing_date:
            needs_facts_download = False

    if needs_facts_download:
        try:
            facts = get_json(session, facts_url, cfg.timeout_s, cfg.rps, log_memory=True)
            facts_bytes = json.dumps(facts, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            del facts
            gc.collect()
            existing_facts_path = None
        except AirflowFailException as e:
            error_msg = str(e)
            if "status=404" in error_msg or "(status=404)" in error_msg or "status= 404" in error_msg:
                facts_bytes = None
                existing_facts_path = None
                needs_facts_download = False
            else:
                raise
    else:
        facts_bytes = None
        existing_facts_path = existing_data.get("companyfacts") if existing_data else None

    submissions_bytes = json.dumps(submissions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    del submissions
    gc.collect()

    if cfg.s3_bucket:
        raise RuntimeError("S3 flow is still handled inside the DAG task wrapper.")
    else:
        cik_dir = os.path.join(cfg.local_dir, f"cik={cik}")
        sub_path = os.path.join(cik_dir, "submissions.json")
        facts_path = os.path.join(cik_dir, "companyfacts.json")

        write_bytes_fn(sub_path, submissions_bytes)

        if facts_bytes is not None:
            write_bytes_fn(facts_path, facts_bytes)
            facts_location = facts_path
        else:
            facts_location = existing_facts_path if existing_facts_path else facts_path

        write_metadata_fn(cik_dir, new_filing_date, ingest_dt)

        result = {
            "cik": cik,
            "ticker": ticker,
            "stored": "local",
            "submissions": sub_path,
            "companyfacts": facts_location,
            "facts_downloaded": facts_bytes is not None,
        }

    return result


def estimate_results_size_mb(results: List[Dict[str, str]]) -> float:
    """Estimate the memory size of results list in MB."""
    if not results:
        return 0.0
    try:
        json_bytes = json.dumps(results, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return len(json_bytes) / (1024.0 * 1024.0)
    except Exception:
        return len(results) * 0.001


def fetch_and_store_companies(
    cfg: Settings,
    session: requests.Session,
    companies: List[Dict[str, str]],
    sec_base: str,
) -> Dict[str, Any]:
    """
    Process companies sequentially.
    
    Downloads and stores JSON files for each company one at a time.
    Includes memory monitoring, incremental file writing, and error handling.
    """
    total_companies = len(companies)
    logger.info("Starting serial processing of %d companies", total_companies)

    mem_initial = get_memory_mb()
    logger.info("Initial memory: %.1f MB", mem_initial)

    results = []
    facts_downloaded_count = 0
    facts_skipped_count = 0
    ciks_needing_ingest = []  # Track CIKs with new data or first-time loads

    # Write results incrementally to a file to avoid losing them when we clear the list
    results_file_path = None
    if cfg.local_dir:
        results_file_path = os.path.join(cfg.local_dir, "processing_results.jsonl")
        if os.path.exists(results_file_path):
            os.remove(results_file_path)
        logger.info("Writing results incrementally to: %s", results_file_path)

    for idx, company in enumerate(companies):
        mem_before = get_memory_mb()

        try:
            # Check if this CIK has existing data before processing
            cik = company["cik"]
            existing_data = find_existing_data(cfg, cik)
            is_first_time = not existing_data or not existing_data.get("metadata")

            result = process_single_company(
                cfg,
                session,
                company,
                sec_base=sec_base,
                find_existing_data_fn=find_existing_data,
                read_metadata_fn=read_metadata,
                write_bytes_fn=write_bytes,
                write_metadata_fn=write_metadata,
            )
            results.append(result)

            # Write result to file immediately
            if results_file_path:
                with open(results_file_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(result, separators=(",", ":"), ensure_ascii=False) + "\n")

            # Track CIKs that need ingestion: new filings OR first-time load
            if result.get("facts_downloaded", False) or is_first_time:
                ciks_needing_ingest.append(pad_cik(cik))

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
            continue

        # Monitor results list size before clearing
        results_count = len(results)
        results_size_mb = estimate_results_size_mb(results)
        results_size_kb = results_size_mb * 1024.0
        logger.info(
            "Results list: %d items, estimated size: %.3f MB (%.1f KB)",
            results_count,
            results_size_mb,
            results_size_kb,
        )

        mem_after = get_memory_mb()
        logger.info(
            "After CIK %s (%d/%d): Memory: %.1f MB (cumulative delta: +%.1f MB from start)",
            company.get("cik", "unknown"),
            idx + 1,
            total_companies,
            mem_after,
            mem_after - mem_initial,
        )

        # Clear results list to test if it's causing memory growth
        mem_before_clear = get_memory_mb()
        results.clear()
        gc.collect()
        mem_after_clear = get_memory_mb()
        freed_mb = mem_before_clear - mem_after_clear
        logger.info(
            "After clearing results list and GC: Memory: %.1f MB (freed %.3f MB from clearing)",
            mem_after_clear,
            freed_mb,
        )

    # Close the requests session to release connection pools
    mem_before_session_close = get_memory_mb()
    session.close()
    gc.collect()
    mem_after_session_close = get_memory_mb()
    logger.info(
        "After closing requests session and GC: Memory: %.1f MB (freed %.3f MB)",
        mem_after_session_close,
        mem_before_session_close - mem_after_session_close,
    )

    mem_final = get_memory_mb()
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
    logger.info(
        "CIKs needing ingestion (new filings or first-time): %d",
        len(ciks_needing_ingest),
    )
    logger.info("=" * 80)

    return {
        "results_file": results_file_path,
        "total_companies": total_companies,
        "facts_downloaded": facts_downloaded_count,
        "facts_skipped": facts_skipped_count,
        "ciks_needing_ingest": ciks_needing_ingest,
    }
