# SEC Scraper DAG

This DAG downloads SEC EDGAR company submissions and company facts, stores raw JSON by CIK, ingests normalized data into PostgreSQL, validates the load, fetches daily ticker prices (Yahoo Finance), and summarizes results.

## Tasks

- **get_company_ciks()**  
  Downloads `company_tickers.json` and returns a filtered list of companies. Applies `SEC_START_CIK` and `SEC_MAX_CIKS_PER_RUN`.

- **fetch_and_store_companies(companies)**  
  For each company: downloads `submissions.json`; checks `metadata.json` for latest filing date; downloads `companyfacts.json` only when new filings exist; writes `metadata.json`; stores JSON locally or in S3. Writes results incrementally to `processing_results.jsonl`.

- **ingest_to_postgres(summary)**  
  Converts JSON to NDJSON, upserts `metric_metadata`, and loads into `sec_raw` tables (submissions, companyfacts_metadata, companyfacts_facts). Skips CIKs already present for the current ingest date (idempotent). Returns processed / errors / skipped counts.

- **validate_postgres_ingestion(ingest_result)**  
  Row count, NULL CIK, array alignment (tickers vs exchanges), data type (fiscal_year), primary key uniqueness. Raises on critical failures.

- **fetch_ticker_prices(validated)**  
  Fetches daily OHLCV for tracked tickers (from `submissions_ticker_mapping`, latest ingest, likely common stock) via Yahoo Finance (yfinance) and upserts into `sec_raw.ticker_prices_daily`. Uses Airflow logical date (`ds`) so backfills get historical prices.

- **summarize(stored)**  
  Logs processing summary (companies, facts downloaded/skipped, results file). Handles both summary-dict and legacy list formats.

## DAG flow

```
get_company_ciks → fetch_and_store_companies → ingest_to_postgres → validate_postgres_ingestion → fetch_ticker_prices → summarize(stored)
```

## Storage layout (local)

`/opt/airflow/data/sec_raw/cik={CIK}/`

- `submissions.json`
- `companyfacts.json` (optional when no new filings)
- `metadata.json`
- `processing_results.jsonl` (run summary)

## Incremental / idempotent behavior

- **Fetch**: `metadata.json.latest_filing_date` determines whether to fetch `companyfacts.json`. `submissions.json` is always refreshed.
- **Ingest**: CIKs that already have a row in `submissions` for the current ingest date are skipped so the same facts are not re-imported on re-runs.
