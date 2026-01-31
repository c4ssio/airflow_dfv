## Project Overview
This repo contains an Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR
company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance),
and validates the load. The primary entry point is the DAG in `dags/sec_scraper.py`.

## Core Data Flow
- Download SEC `company_tickers.json` to enumerate CIKs.
- For each CIK:
  - Always fetch `submissions/CIK*.json`.
  - Fetch `companyfacts/CIK*.json` only when new filings are detected.
  - Store raw JSON locally (`/opt/airflow/data/sec_raw`) or to S3.
  - Write `metadata.json` per CIK to support incremental updates.
- Convert JSON to NDJSON and load into PostgreSQL `sec_raw` tables (submissions, companyfacts_metadata, companyfacts_facts, metric_metadata). Ingestion skips CIKs already present for the current ingest date (idempotent).
- After validation, fetch daily ticker prices for tracked tickers (from `submissions_ticker_mapping`) via Yahoo Finance (yfinance) and upsert into `sec_raw.ticker_prices_daily`.

## Key Files
- `dags/sec_scraper.py`: Airflow DAG and thin task wrappers.
- `plugins/scripts/sec_scraper/`: Task logic (fetch_company_ciks, fetch_and_store_companies, ingest_to_postgres, validate_postgres_ingestion, fetch_ticker_prices), common, storage, postgres helpers.
- `plugins/scripts/sec_scraper/postgres/migrations/`: PostgreSQL DDL.
- `plugins/scripts/sec_scraper/postgres/deploy_migrations.py`: migration runner.
- `scripts/`: Operational helpers (venv, Airflow checks, memory, restart).
- `scripts/adhoc/`: Manual/ad-hoc scripts (price coverage SQL, Yahoo missing-tickers diagnostic). See `scripts/adhoc/README.md`.
- `mermaid.md`: Architecture diagram.

## Database Schema (sec_raw)
- `submissions`: company metadata with JSONB arrays for tickers, exchanges, filings.
- `companyfacts_metadata`: per-company facts metadata.
- `companyfacts_facts`: normalized financial metrics (no label/description columns).
- `metric_metadata`: canonical metric reference table (taxonomy + metric_name PK). Labels and descriptions live here; joined via `companyfacts_facts_full` view.
- `ticker_prices_daily`: daily OHLCV by (ticker, price_date); source e.g. "yahoo". Used for cheapness/valuation.
- Views: `companyfacts_facts_full`, `companyfacts_facts_with_abbrev` (alias), `submissions_ticker_mapping`.

## Configuration
- `SEC_USER_AGENT` is required by SEC (include a real email).
- `SEC_REQUESTS_PER_SECOND`, `SEC_TIMEOUT_SECONDS`, `SEC_MAX_CIKS_PER_RUN`, `SEC_START_CIK` control rate and scope.
- `SEC_S3_BUCKET`/`SEC_S3_PREFIX` enable S3 storage (optional).
- `config/postgres.yaml` configures PostgreSQL ingestion.
- Ticker prices: Yahoo Finance (yfinance). Optional: `SEC_PRICE_DATE`, `SEC_MAX_TICKERS_PER_RUN`.

## Database Setup
The PostgreSQL database is provisioned automatically via Docker Compose.
A separate `sec_data` database is created with schema `sec_raw`.

To install new Python dependencies (e.g. after adding to `requirements.txt`), rebuild the Airflow image:
```bash
docker compose build
docker compose up -d
```

To run migrations manually:
```bash
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

## Code Style Guidelines
- **Avoid meaningless qualifiers**: Don't use "all" in function/method names (e.g., use `fetch_companies` not `fetch_all_companies`, `ingest_ciks` not `ingest_all_ciks`).
- **Keep DAG files thin**: Extract business logic to `plugins/scripts/sec_scraper/` modules. DAG files should contain only task wrappers and orchestration.
- **Testability**: Functions should accept dependencies as parameters (dependency injection) rather than importing them directly, making them easier to unit test.

## Notes for Changes
- Do not commit secrets from `config/secrets.env` or `config/postgres.yaml`.
- Raw data in `data/` and logs in `logs/` are local-only.
- If you change PostgreSQL schemas, update migration SQL and any NDJSON mapping.
