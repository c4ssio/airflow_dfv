## SEC Scraper DAG Intent
This DAG downloads SEC EDGAR company submissions and company facts, stores raw
JSON by CIK, and ingests normalized data into PostgreSQL.

## Tasks
- `get_company_ciks()`
  - Downloads `company_tickers.json` and returns a filtered CIK list.
  - Applies `SEC_START_CIK` and `SEC_MAX_CIKS_PER_RUN`.
- `fetch_and_store_all_companies()`
  - Always downloads `submissions.json`.
  - Checks `metadata.json` to detect new filings.
  - Downloads `companyfacts.json` only when new filings exist.
  - Writes `metadata.json` with `latest_filing_date` and `last_updated`.
  - Stores JSON locally or in S3.
- `ingest_to_postgres()`
  - Converts JSON to NDJSON files.
  - Upserts `metric_metadata` (label, description) for each metric encountered.
  - Loads into PostgreSQL `sec_raw` tables using DELETE + INSERT per file.
  - Commits per file for progress visibility and failure resilience.
- `validate_postgres_ingestion()`
  - Row count validation for submissions table.
  - NULL check on required CIK field.
  - Array alignment check (tickers vs exchanges JSONB arrays).
  - Data type validation (fiscal_year numeric).
  - Primary key uniqueness check.
  - Raises `AirflowFailException` for critical failures.
- `summarize()`
  - Logs counts for processed companies and facts downloaded vs skipped.

## DAG Flow
```
get_company_ciks → fetch_and_store_all_companies → ingest_to_postgres → validate_postgres_ingestion → summarize
```

## Normalized Schema
The `companyfacts_facts` table does not store metric labels or descriptions.
These fields live in the `metric_metadata` reference table (keyed by taxonomy +
metric_name) and are joined via the `companyfacts_facts_full` view.

During ingestion, new metrics are upserted into `metric_metadata` so labels and
descriptions stay current as new metrics appear in SEC filings.

## Storage Layout (Local)
`/opt/airflow/data/sec_raw/cik={CIK}/`
- `submissions.json`
- `companyfacts.json` (optional when no new filings)
- `metadata.json`
- `processing_results.jsonl` (run summary entries)

## Incremental Behavior
- Uses `metadata.json.latest_filing_date` to decide whether to fetch
  `companyfacts.json` for a CIK.
- `submissions.json` is always refreshed.
