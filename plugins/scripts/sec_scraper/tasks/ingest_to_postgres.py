from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

from scripts.sec_scraper.common import (
    Settings,
    convert_companyfacts_to_ndjson,
    convert_submissions_to_ndjson,
)
from scripts.sec_scraper.postgres.helpers import (
    get_ciks_already_ingested,
    get_postgres_config,
    get_postgres_connection,
    load_ndjson_batch_to_postgres,
    upsert_metric_metadata,
)


def _discover_cik_dirs(cfg: Settings) -> List[str]:
    base_dir = cfg.local_dir
    if not os.path.exists(base_dir):
        return []

    cik_dirs = [
        d
        for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d)) and d.startswith("cik=")
    ]

    # Filter to specific CIK if testing
    if cfg.ingest_test_cik:
        test_cik_dir = f"cik={cfg.ingest_test_cik}"
        if test_cik_dir in cik_dirs:
            return [test_cik_dir]
        return []

    if cfg.ingest_max_ciks > 0:
        cik_dirs = sorted(cik_dirs)[: cfg.ingest_max_ciks]

    return sorted(cik_dirs)


def _build_ndjson_for_cik(
    base_dir: str,
    cik_dir_name: str,
    ingest_date: str,
) -> Tuple[
    List[str],
    List[str],
    List[str],
    List[str],
    List[str],
    List[str],
]:
    """Given a single CIK directory, produce NDJSON file paths and CIK lists.

    Returns:
        (submission_paths, metadata_paths, facts_paths,
         ciks_submissions, ciks_metadata, ciks_facts)
    """
    cik = cik_dir_name.replace("cik=", "")
    cik_path = os.path.join(base_dir, cik_dir_name)

    submissions_json = os.path.join(cik_path, "submissions.json")
    companyfacts_json = os.path.join(cik_path, "companyfacts.json")

    batch_submission_paths: List[str] = []
    batch_metadata_paths: List[str] = []
    batch_facts_paths: List[str] = []
    batch_ciks_submissions: List[str] = []
    batch_ciks_metadata: List[str] = []
    batch_ciks_facts: List[str] = []

    # Submissions
    if os.path.exists(submissions_json):
        with open(submissions_json, "r", encoding="utf-8") as f:
            submissions_data = json.load(f)

        submissions_row = convert_submissions_to_ndjson(
            submissions_data, cik, ingest_date
        )

        submissions_ndjson = os.path.join(cik_path, "submissions.ndjson")
        _write_ndjson_file(submissions_ndjson, [submissions_row])
        batch_submission_paths.append(submissions_ndjson)
        batch_ciks_submissions.append(cik)

    # Companyfacts
    if os.path.exists(companyfacts_json):
        with open(companyfacts_json, "r", encoding="utf-8") as f:
            companyfacts_data = json.load(f)

        metadata_rows, facts_rows, _ = convert_companyfacts_to_ndjson(
            companyfacts_data, cik, ingest_date
        )

        metadata_ndjson = os.path.join(cik_path, "companyfacts_metadata.ndjson")
        facts_ndjson = os.path.join(cik_path, "companyfacts_facts.ndjson")

        _write_ndjson_file(metadata_ndjson, metadata_rows)
        _write_ndjson_file(facts_ndjson, facts_rows)

        batch_metadata_paths.append(metadata_ndjson)
        batch_facts_paths.append(facts_ndjson)
        batch_ciks_metadata.append(cik)
        batch_ciks_facts.append(cik)

    return (
        batch_submission_paths,
        batch_metadata_paths,
        batch_facts_paths,
        batch_ciks_submissions,
        batch_ciks_metadata,
        batch_ciks_facts,
    )


def _write_ndjson_file(file_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _cik_from_dir_name(cik_dir_name: str) -> str:
    """Zero-padded 10-digit CIK for comparison with DB."""
    cik = cik_dir_name.replace("cik=", "").lstrip("0") or "0"
    return cik.zfill(10)


def ingest_ciks(
    cfg: Settings,
    postgres_config: Dict[str, Any],
    load_fn,
    upsert_metric_metadata_fn,
) -> Dict[str, Any]:
    """Core ingestion workflow, parameterized by DB functions for easier testing."""
    base_dir = cfg.local_dir
    cik_dirs = _discover_cik_dirs(cfg)

    if not cik_dirs:
        return {"total_ciks": 0, "processed": 0, "errors": 0, "skipped": 0}

    ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
    schema = postgres_config.get("schema", "sec_raw")

    processed = 0
    errors = 0

    # Open a single PostgreSQL connection for all processing
    conn = None
    try:
        conn = load_fn.get_connection(postgres_config, schema)

        # Skip CIKs already imported for this ingest_date (idempotent runs)
        already_ingested = get_ciks_already_ingested(conn, schema, ingest_date)
        cik_dirs_to_process = [
            d for d in cik_dirs
            if _cik_from_dir_name(d) not in already_ingested
        ]
        skipped = len(cik_dirs) - len(cik_dirs_to_process)

        batch_size = 500

        for i in range(0, len(cik_dirs_to_process), batch_size):
            batch_dirs = sorted(cik_dirs_to_process)[i : i + batch_size]

            batch_submission_paths: List[str] = []
            batch_metadata_paths: List[str] = []
            batch_facts_paths: List[str] = []
            batch_ciks_submissions: List[str] = []
            batch_ciks_metadata: List[str] = []
            batch_ciks_facts: List[str] = []

            for cik_dir_name in batch_dirs:
                try:
                    (
                        sub_paths,
                        meta_paths,
                        facts_paths,
                        ciks_sub,
                        ciks_meta,
                        ciks_facts,
                    ) = _build_ndjson_for_cik(base_dir, cik_dir_name, ingest_date)

                    batch_submission_paths.extend(sub_paths)
                    batch_metadata_paths.extend(meta_paths)
                    batch_facts_paths.extend(facts_paths)
                    batch_ciks_submissions.extend(ciks_sub)
                    batch_ciks_metadata.extend(ciks_meta)
                    batch_ciks_facts.extend(ciks_facts)

                    processed += 1
                except Exception:
                    errors += 1
                    continue

            if batch_submission_paths:
                load_fn.load_ndjson_batch(
                    conn,
                    "submissions",
                    batch_submission_paths,
                    batch_ciks_submissions,
                    ingest_date,
                    schema,
                )

            if batch_metadata_paths:
                load_fn.load_ndjson_batch(
                    conn,
                    "companyfacts_metadata",
                    batch_metadata_paths,
                    batch_ciks_metadata,
                    ingest_date,
                    schema,
                )

            if batch_facts_paths:
                load_fn.load_ndjson_batch(
                    conn,
                    "companyfacts_facts",
                    batch_facts_paths,
                    batch_ciks_facts,
                    ingest_date,
                    schema,
                )

    finally:
        if conn:
            conn.close()

    return {
        "total_ciks": len(cik_dirs),
        "processed": processed,
        "errors": errors,
        "skipped": skipped,
    }


def ingest_to_postgres(cfg: Settings) -> Dict[str, Any]:
    """
    Ingest all CIKs to PostgreSQL.
    
    Loads PostgreSQL config, creates connection helpers, and delegates to ingest_ciks.
    This is the main entry point for the DAG task.
    """
    # Load PostgreSQL config
    try:
        postgres_config = get_postgres_config(cfg.postgres_config_path)
    except Exception as e:
        raise RuntimeError(f"PostgreSQL config error: {e}")

    # Create load functions wrapper
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

    return ingest_ciks(
        cfg=cfg,
        postgres_config=postgres_config,
        load_fn=_LoadFns,
        upsert_metric_metadata_fn=upsert_metric_metadata,
    )

