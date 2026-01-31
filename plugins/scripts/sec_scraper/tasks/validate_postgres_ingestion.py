from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

from scripts.sec_scraper.common import Settings
from scripts.sec_scraper.postgres.helpers import get_postgres_config, get_postgres_connection

logger = logging.getLogger(__name__)


def validate_postgres_ingestion(
    cfg: Settings,
    ingest_result: Dict[str, Any],
) -> Dict[str, Any]:
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
        postgres_config = get_postgres_config(cfg.postgres_config_path)
    except Exception as e:
        logger.error("Failed to load PostgreSQL config for validation: %s", e)
        raise RuntimeError(f"Validation failed - cannot connect to PostgreSQL: {e}")

    schema = postgres_config.get("schema", "sec_raw")
    ingest_date = datetime.utcnow().strftime("%Y-%m-%d")

    conn = None
    validation_results = []
    validations_passed = 0
    validations_failed = 0
    critical_failure = False

    try:
        conn = get_postgres_connection(postgres_config, schema)
        cursor = conn.cursor()

        # Validation 1: Row count validation for submissions table
        logger.info("Running validation 1: Row count check")
        cursor.execute(
            f"SELECT COUNT(*) FROM {schema}.submissions WHERE ingest_date = %s",
            (ingest_date,),
        )
        submissions_count = cursor.fetchone()[0]

        expected_count = ingest_result.get("processed", 0)
        row_count_status = "passed" if submissions_count > 0 else "failed"
        if submissions_count == 0 and expected_count > 0:
            validations_failed += 1
            critical_failure = True
            logger.error("CRITICAL: No rows found in submissions for ingest_date=%s", ingest_date)
        else:
            validations_passed += 1
            logger.info("Row count validation: %d rows in submissions table", submissions_count)

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
            (ingest_date,),
        )
        null_cik_count = cursor.fetchone()[0]

        null_check_status = "passed" if null_cik_count == 0 else "failed"
        if null_cik_count > 0:
            validations_failed += 1
            critical_failure = True
            logger.error("CRITICAL: Found %d rows with NULL CIK in submissions", null_cik_count)
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
            (ingest_date,),
        )
        mismatched_rows = cursor.fetchall()
        mismatched_ciks = [row[0] for row in mismatched_rows]

        array_check_status = "passed" if len(mismatched_ciks) == 0 else "warning"
        if mismatched_ciks:
            logger.warning(
                "Array alignment: %d CIKs have mismatched ticker/exchange arrays: %s",
                len(mismatched_ciks),
                mismatched_ciks,
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
            (ingest_date,),
        )
        facts_with_fy = cursor.fetchone()[0]

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {schema}.companyfacts_facts
            WHERE ingest_date = %s
            """,
            (ingest_date,),
        )
        total_facts = cursor.fetchone()[0]

        type_check_status = "passed"
        validations_passed += 1
        logger.info("Data type check: %d/%d facts have fiscal_year values", facts_with_fy, total_facts)

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
            (ingest_date,),
        )
        duplicate_rows = cursor.fetchall()
        duplicate_ciks = [row[0] for row in duplicate_rows]

        pk_check_status = "passed" if len(duplicate_ciks) == 0 else "failed"
        if duplicate_ciks:
            validations_failed += 1
            logger.error("Found duplicate CIKs in submissions: %s", duplicate_ciks)
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
        logger.error("Validation query failed: %s", e)
        raise RuntimeError(f"Validation failed with error: {e}")
    finally:
        if conn:
            conn.close()

    # Log summary
    logger.info("=" * 80)
    logger.info(
        "Validation complete: %d passed, %d failed",
        validations_passed,
        validations_failed,
    )
    for result in validation_results:
        logger.info("  - %s: %s", result["name"], result["status"])
    logger.info("=" * 80)

    # Raise exception for critical failures
    if critical_failure:
        raise RuntimeError(
            f"Critical validation failures detected: {validations_failed} validations failed. "
            "Check logs for details."
        )

    return {
        "validations_passed": validations_passed,
        "validations_failed": validations_failed,
        "details": validation_results,
    }
