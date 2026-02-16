"""Helpers for recording pipeline runs in sec_raw.pipeline_run_log."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def record_completed_run(
    conn: Any,
    schema: str,
    run_date: str,
    summary: Optional[Dict[str, Any]] = None,
) -> int:
    """Insert a completed-run row and return the new run_id."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {schema}.pipeline_run_log
                (run_date, started_at, completed_at, status, summary)
            VALUES
                (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'success', %s)
            RETURNING run_id
            """,
            (run_date, json.dumps(summary) if summary else None),
        )
        row = cur.fetchone()
        conn.commit()
        run_id = row[0] if row else 0
        logger.info("Recorded pipeline run run_id=%d run_date=%s", run_id, run_date)
        return run_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def get_last_successful_run_date(conn: Any, schema: str) -> Optional[str]:
    """Return the most recent run_date with status='success', or None."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT MAX(run_date) FROM {schema}.pipeline_run_log WHERE status = 'success'"
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        return None
    finally:
        cur.close()
