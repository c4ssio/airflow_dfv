## Project Overview

This repo contains an Airflow (CeleryExecutor) pipeline that downloads SEC EDGAR
company data, ingests it into PostgreSQL, fetches daily ticker prices (Yahoo Finance),
and validates the load. The primary entry point is the DAG in `dags/sec_scraper.py`.

**Stack:** Python 3.11, Apache Airflow 3.1.5 (CeleryExecutor), PostgreSQL 15, Redis 7.

## Directory Structure

```
airflow_dfv/
├── dags/
│   ├── sec_scraper.py              # Airflow DAG — thin task wrappers only
│   └── sec_scraper.md              # DAG documentation
├── plugins/scripts/sec_scraper/    # All business logic lives here
│   ├── common.py                   # Settings, HTTP helpers, rate limiter, data converters
│   ├── storage.py                  # Local/S3 read/write helpers
│   ├── tasks/
│   │   ├── fetch_company_ciks.py
│   │   ├── fetch_and_store_companies.py
│   │   ├── ingest_to_postgres.py
│   │   ├── validate_postgres_ingestion.py
│   │   └── fetch_ticker_prices.py
│   ├── postgres/
│   │   ├── helpers.py              # Connection, NDJSON loading, upsert helpers
│   │   ├── deploy_migrations.py    # Migration runner (--dry-run, --verbose, --rollback)
│   │   ├── migrations/             # SQL DDL files (YYYYMMDDHHMM__description.sql)
│   │   └── README.md
│   └── tests/
│       ├── test_common_converters.py
│       └── test_fetch_company_ciks.py
├── scripts/
│   ├── setup_venv.sh               # Create Python venv on the host
│   ├── run_with_venv.sh            # Run commands inside the venv
│   ├── check_airflow.sh            # Airflow diagnostics → diagnostics/
│   ├── restart_airflow_services.sh
│   ├── check_worker_memory.sh
│   ├── monitor_worker_memory.sh
│   ├── cleanup_removed_tasks.py    # Remove stale Airflow task metadata
│   ├── init-sec-db.sql             # Creates sec_data DB on first Postgres startup
│   └── adhoc/                      # One-off diagnostics (SQL, Python)
├── config/                         # gitignored — secrets.env, postgres.yaml
├── compose.yaml                    # Docker Compose (all services)
├── Dockerfile                      # Extends apache/airflow:3.1.5
├── requirements.txt                # Python dependencies
├── pytest.ini                      # pytest configuration
├── pyrightconfig.json              # Pyright type checking (basic mode)
└── mermaid.md                      # Architecture diagram
```

## Core Data Flow

1. Download SEC `company_tickers.json` to enumerate CIKs.
2. For each CIK:
   - Always fetch `submissions/CIK*.json`.
   - Fetch `companyfacts/CIK*.json` only when new filings are detected.
   - Store raw JSON locally (`/opt/airflow/data/sec_raw`) or to S3.
   - Write `metadata.json` per CIK to support incremental updates.
3. Convert JSON to NDJSON and load into PostgreSQL `sec_raw` tables (submissions, companyfacts_metadata, companyfacts_facts, metric_metadata). Ingestion skips CIKs already present for the current ingest date (idempotent).
4. Run data integrity validations (row counts, NULL checks, PK uniqueness, type checks, array alignment).
5. Fetch daily ticker prices for tracked tickers (from `submissions_ticker_mapping`) via Yahoo Finance (yfinance) and upsert into `sec_raw.ticker_prices_daily`.

## DAG Task Sequence

```
get_company_ciks → fetch_and_store_companies → ingest_to_postgres → validate_postgres_ingestion → fetch_ticker_prices → summarize
```

- **Schedule:** `0 6 * * *` (daily at 06:00), `catchup=False`, `max_active_runs=1`.
- **Default retries:** 2 with 2-minute delay.
- `fetch_ticker_prices` uses the Airflow logical date (`ds`) so backfills pull historical prices.

## Database Schema (sec_raw)

**Database:** `sec_data` | **Schema:** `sec_raw`

### Tables
| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `submissions` | (cik, ingest_date) | Company metadata; JSONB arrays for tickers, exchanges, filings |
| `companyfacts_metadata` | (cik, ingest_date) | Per-company facts metadata |
| `companyfacts_facts` | (cik, ingest_date, taxonomy, metric_name, unit, period_end, accession_number) | Normalized financial metrics (no label/description columns) |
| `metric_metadata` | (taxonomy, metric_name) | Canonical metric reference; labels and descriptions live here |
| `ticker_prices_daily` | (ticker, price_date) | Daily OHLCV; source e.g. "yahoo" |
| `schema_migrations` | (migration_name) | Migration tracking with MD5 checksums |

### Views
- `companyfacts_facts_full` — facts joined with metric_metadata (label, description, abbreviation, category).
- `companyfacts_facts_with_abbrev` — backward-compatible alias for `companyfacts_facts_full`.
- `submissions_ticker_mapping` — expands JSONB ticker/exchange arrays into rows with `is_primary_ticker`, `is_likely_common_stock`, `share_class_indicator`.

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SEC_USER_AGENT` | Yes | — | Email-based identifier required by SEC |
| `SEC_REQUESTS_PER_SECOND` | No | `5` | Rate limit for SEC API |
| `SEC_TIMEOUT_SECONDS` | No | `30` | HTTP request timeout |
| `SEC_MAX_CIKS_PER_RUN` | No | `250` | Max companies per DAG run |
| `SEC_START_CIK` | No | `""` | Start from a specific CIK |
| `SEC_LOCAL_DIR` | No | `/tmp/sec_raw` | Local storage path |
| `SEC_S3_BUCKET` | No | `""` | S3 bucket (empty = local only) |
| `SEC_S3_PREFIX` | No | `sec_raw` | S3 key prefix |
| `SEC_INGEST_TEST_CIK` | No | `""` | Test with a single CIK |
| `SEC_INGEST_MAX_CIKS` | No | `0` | Limit CIKs during ingestion (0 = no limit) |
| `SEC_PRICE_DATE` | No | today | Override price fetch date |
| `SEC_MAX_TICKERS_PER_RUN` | No | unlimited | Limit tickers for price fetch |
| `POSTGRES_CONFIG_PATH` | No | auto-detected | Path to `postgres.yaml` |

### Postgres Config (`config/postgres.yaml`, gitignored)

```yaml
host: postgres
port: 5432
database: sec_data
user: airflow
password: airflow
schema: sec_raw
```

## Development Setup

### Starting the Stack

```bash
docker compose build          # Build Airflow image with dependencies
docker compose up -d          # Start all services
```

Airflow UI: http://localhost:8080

### Installing New Python Dependencies

Add to `requirements.txt`, then rebuild:

```bash
docker compose build && docker compose up -d
```

### Running Migrations

```bash
# Inside Docker (recommended)
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py

# Preview only
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py --dry-run

# From host with venv
./scripts/run_with_venv.sh python plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

### Migration File Format

Files must follow `YYYYMMDDHHMM__description.sql` naming. Migrations run in chronological order by filename prefix. Optional rollback files: `YYYYMMDDHHMM__description__rollback.sql`.

## Testing

### Running Tests

```bash
# From project root (host, with venv)
./scripts/run_with_venv.sh python -m pytest

# Inside Docker
docker compose exec airflow-worker python -m pytest /opt/airflow/plugins/scripts/sec_scraper/tests/

# Single test file
./scripts/run_with_venv.sh python -m pytest plugins/scripts/sec_scraper/tests/test_common_converters.py -v
```

### Test Conventions

- Tests live in `plugins/scripts/sec_scraper/tests/`.
- pytest is configured via `pytest.ini` (testpaths, pythonpath).
- Use dependency injection: pass `Settings` and `requests.Session` as parameters so tests can supply fakes (see `DummySession` in `test_fetch_company_ciks.py`).
- Test function names should describe the behavior being tested: `test_<function>_<scenario>`.

### Type Checking

Pyright is configured in `pyrightconfig.json` (basic mode, Python 3.11). Includes `dags/`, `plugins/`, `scripts/` with `plugins/` as an extra path.

## Code Style Guidelines

- **Avoid meaningless qualifiers**: Don't use "all" in function/method names (e.g., use `fetch_companies` not `fetch_all_companies`, `ingest_ciks` not `ingest_all_ciks`).
- **Keep DAG files thin**: Extract business logic to `plugins/scripts/sec_scraper/` modules. DAG files should contain only task wrappers and orchestration.
- **Testability**: Functions should accept dependencies as parameters (dependency injection) rather than importing them directly, making them easier to unit test.
- **Import convention**: In the DAG file, import task functions inside the `@task` wrapper to avoid import-time side effects. Module-level imports are fine for `common.py` utilities.
- **Settings**: All configuration flows through the `Settings` dataclass in `common.py`. Never read `os.environ` directly in task modules—use `Settings` fields instead.
- **CIK format**: CIKs are zero-padded to 10 digits via `pad_cik()` before storage or database operations.

## Docker Architecture

Services defined in `compose.yaml`:

| Service | Purpose |
|---------|---------|
| `postgres` | PostgreSQL 15 — Airflow metadata DB + `sec_data` DB |
| `redis` | Celery broker |
| `airflow-init` | One-shot: runs `airflow db migrate` |
| `airflow-api-server` | API/UI server (port 8080) |
| `airflow-scheduler` | Task scheduler |
| `airflow-dag-processor` | DAG parser |
| `airflow-worker` | Celery task executor (concurrency: 2) |
| `airflow-triggerer` | Event-based triggers |

Volumes mount `dags/`, `plugins/`, `data/`, `config/`, and `logs/` into containers at `/opt/airflow/`.

The `sec_data` database is auto-created via `scripts/init-sec-db.sql` (mounted into Postgres `docker-entrypoint-initdb.d`).

## Notes for Changes

- **Secrets**: Do not commit `config/secrets.env` or `config/postgres.yaml`. The entire `config/` directory is gitignored except `*.example` files.
- **Local-only data**: `data/` (raw JSON) and `logs/` (Airflow logs) are gitignored.
- **Schema changes**: If you change PostgreSQL schemas, add a new migration SQL file and update any NDJSON conversion logic in `common.py` or `ingest_to_postgres.py`.
- **New tasks**: Create the task module in `plugins/scripts/sec_scraper/tasks/`, then add a thin `@task` wrapper in the DAG file.
- **Diagnostics output**: Scripts that produce diagnostic output write to `diagnostics/` (also gitignored).
