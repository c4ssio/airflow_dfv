# airflow_dfv

Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance), and validates the load.

## What it does

- **SEC data**: Downloads company submissions and company facts JSON from SEC EDGAR, stores raw JSON by CIK, converts to NDJSON and loads into `sec_raw` (submissions, companyfacts_metadata, companyfacts_facts, metric_metadata).
- **Ticker prices**: Fetches daily OHLCV for tracked tickers (from `submissions_ticker_mapping`) via Yahoo Finance and upserts into `sec_raw.ticker_prices_daily` for cheapness/valuation analysis.
- **Idempotent ingest**: Skips CIKs already present in `submissions` for the current ingest date so re-runs don’t re-import the same facts.

## Quick start

- **Run Airflow**: `docker compose up -d` (after `docker compose build` if you changed `requirements.txt`).
- **Run migrations**: `docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py`
- **Ad-hoc scripts**: See `scripts/adhoc/README.md` (price coverage SQL, Yahoo missing-tickers diagnostic).

## Key paths

- **DAG**: `dags/sec_scraper.py`
- **Logic**: `plugins/scripts/sec_scraper/` (tasks, common, storage, postgres)
- **Migrations**: `plugins/scripts/sec_scraper/postgres/migrations/`
- **Operational scripts**: `scripts/` (venv, Airflow checks, memory, restart)
- **Ad-hoc / manual scripts**: `scripts/adhoc/`

## Config

- **SEC**: `SEC_USER_AGENT` (required), `SEC_REQUESTS_PER_SECOND`, `SEC_MAX_CIKS_PER_RUN`, `SEC_START_CIK`, `SEC_S3_BUCKET` / `SEC_S3_PREFIX` (optional).
- **Postgres**: `config/postgres.yaml` (host, database `sec_data`, schema `sec_raw`).
- **Prices**: Yahoo Finance (yfinance); optional `SEC_PRICE_DATE`, `SEC_MAX_TICKERS_PER_RUN`.

See `CLAUDE.md` for detailed project notes, schema, and style guidelines.
