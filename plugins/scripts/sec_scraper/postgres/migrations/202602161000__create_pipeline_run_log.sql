-- Pipeline run log: tracks each DAG run start/end/status.
-- Used by backfill logic to determine where to resume.

CREATE TABLE IF NOT EXISTS sec_raw.pipeline_run_log (
    run_id       SERIAL PRIMARY KEY,
    run_date     DATE NOT NULL,
    started_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status       TEXT NOT NULL DEFAULT 'running',   -- running | success | failed
    summary      JSONB
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_status
    ON sec_raw.pipeline_run_log (status, run_date DESC);
