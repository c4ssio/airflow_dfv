# airflow_dfv

Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance), and validates the load.

## Getting up and running (new machine)

**Prerequisites:** Docker and Docker Compose.

1. **Clone and enter the repo**
   ```bash
   cd /path/to/airflow_dfv
   ```

2. **Create config directory and required files** (the `config/` folder is gitignored; you must create it and the files below).
   - **`config/secrets.env`**  
     Environment overrides. Loaded by compose via `env_file`. For API/auth to work across scheduler, API server, and worker, set (same values on all components):
     - `AIRFLOW__API_AUTH__JWT_SECRET` — JWT signing/verification.
     - `AIRFLOW__API__SECRET_KEY` — API server secret (e.g. log fetch from workers).
     Also recommended: `SEC_USER_AGENT="YourApp (your@email.com)"` (required by SEC), `AIRFLOW_UID=$(id -u)` on Linux.
   - **`config/postgres.yaml`**  
     PostgreSQL config for the `sec_data` database (used by the pipeline and migrations). Use this when running via Docker:
     ```yaml
     host: postgres
     port: 5432
     database: sec_data
     user: airflow
     password: airflow
     schema: sec_raw
     ```
   - **`config/simple_auth_manager_passwords.json.generated`**  
     Airflow Simple Auth Manager stores generated passwords here. Create an empty file so the volume mount works: `touch config/simple_auth_manager_passwords.json.generated`.  
     For local dev you can skip login by adding to `config/secrets.env`:  
     `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`

3. **Build and start services**
   ```bash
   docker compose build
   docker compose up -d
   ```

4. **Initialize Airflow metadata DB** (first run only)  
   If the UI or scheduler complain about the DB, run:
   ```bash
   docker compose run --rm airflow-init
   ```

5. **Run SEC schema migrations** (creates `sec_raw` tables in `sec_data`)
   ```bash
   docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py
   ```

6. **Open the UI**  
   http://localhost:8080  
   Log in with the users/passwords from `config/simple_auth_manager_passwords.json.generated` (or webserver logs), or with “everyone is admin” if you set `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` in step 2.

**Optional:** For running Python/adhoc scripts on the host (e.g. migrations, ad-hoc checks), create a venv: `./scripts/setup_venv.sh`. See `scripts/README.md` and `scripts/adhoc/README.md`.

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

## Troubleshooting

- **Task fails with no logs**  
  Check the worker: `docker compose logs airflow-worker`. Common causes: worker OOM (increase memory or reduce `SEC_MAX_CIKS_PER_RUN` / worker concurrency). After changing env or compose, restart: `./scripts/restart_airflow_services.sh` or `docker compose up -d`.

See `CLAUDE.md` for detailed project notes, schema, and style guidelines.
