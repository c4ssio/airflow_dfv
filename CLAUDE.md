## Project Overview
This repo contains an Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR
company data and ingests it into PostgreSQL. The primary entry point
is the DAG in `dags/sec_scraper.py`.

## Core Data Flow
- Download SEC `company_tickers.json` to enumerate CIKs.
- For each CIK:
  - Always fetch `submissions/CIK*.json`.
  - Fetch `companyfacts/CIK*.json` only when new filings are detected.
  - Store raw JSON locally (`/opt/airflow/data/sec_raw`) or to S3.
  - Write `metadata.json` per CIK to support incremental updates.
- Convert JSON to NDJSON and load into PostgreSQL `sec_raw` tables.

## Key Files
- `dags/sec_scraper.py`: Airflow DAG and ingestion logic.
- `plugins/scripts/sec_scraper/postgres/migrations/`: PostgreSQL DDL.
- `plugins/scripts/sec_scraper/postgres/deploy_migrations.py`: migration runner.
- `scripts/`: operational helpers for Airflow and memory diagnostics.
- `mermaid.md`: architecture diagram.

## Database Schema (sec_raw)
- `submissions`: company metadata with JSONB arrays for tickers, exchanges, filings.
- `companyfacts_metadata`: per-company facts metadata.
- `companyfacts_facts`: normalized financial metrics (no label/description columns).
- `metric_metadata`: canonical metric reference table (taxonomy + metric_name PK).
  Labels and descriptions live here; joined via `companyfacts_facts_full` view.
- Views: `companyfacts_facts_full`, `companyfacts_facts_with_abbrev` (alias),
  `submissions_ticker_mapping`.

## Configuration
- `SEC_USER_AGENT` is required by SEC (include a real email).
- `SEC_REQUESTS_PER_SECOND`, `SEC_TIMEOUT_SECONDS`, `SEC_MAX_CIKS_PER_RUN`,
  `SEC_START_CIK` control rate and scope.
- `SEC_S3_BUCKET`/`SEC_S3_PREFIX` enable S3 storage (optional).
- `config/postgres.yaml` configures PostgreSQL ingestion.

## Database Setup
The PostgreSQL database is provisioned automatically via Docker Compose.
A separate `sec_data` database is created with schema `sec_raw`.

To run migrations manually:
```bash
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

## Notes for Changes
- Do not commit secrets from `config/secrets.env` or `config/postgres.yaml`.
- Raw data in `data/` and logs in `logs/` are local-only.
- If you change PostgreSQL schemas, update migration SQL and any NDJSON mapping.
