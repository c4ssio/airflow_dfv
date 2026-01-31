# PostgreSQL Migration System

This directory contains SQL migration files and the migration runner for the SEC scraper PostgreSQL schema (`sec_raw`).

## Files

- `migrations/` - SQL migration files (YYYYMMDDHHMM__description.sql format)
- `deploy_migrations.py` - Migration runner script

## Setup

The PostgreSQL database (`sec_data`) is provisioned automatically by Docker Compose via `scripts/init-sec-db.sql`. The `sec_raw` schema is created by the migration runner on first use.

Configuration is in `config/postgres.yaml`:
```yaml
host: postgres
port: 5432
database: sec_data
user: airflow
password: airflow
schema: sec_raw
```

## Running Migrations

### From inside Docker (recommended)
```bash
docker compose exec airflow-worker python /opt/airflow/plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

### From host (with venv)
```bash
./scripts/run_with_venv.sh python plugins/scripts/sec_scraper/postgres/deploy_migrations.py
```

### Options
```bash
# Preview what would run
python deploy_migrations.py --dry-run

# Verbose logging
python deploy_migrations.py --verbose

# Rollback a specific migration
python deploy_migrations.py --rollback 202512221000__create_submissions

# Custom config path
python deploy_migrations.py --config /path/to/postgres.yaml
```

## Migrations

| Migration | Description |
|-----------|-------------|
| `202512221000__create_submissions` | Submissions table with JSONB arrays |
| `202512221100__create_companyfacts` | companyfacts_metadata and companyfacts_facts tables |
| `202512221200__create_submissions_ticker_mapping` | View expanding ticker/exchange arrays to rows |
| `202512221300__create_us_gaap_metric_abbreviations` | Initial metric abbreviations reference table |
| `202501231500__normalize_metric_metadata` | Renames to metric_metadata, adds label/description, creates companyfacts_facts_full view, removes redundant columns from facts |
| `202601281200__create_ticker_prices_daily` | Daily OHLCV by (ticker, price_date); source e.g. yahoo |

## Current Schema

### Tables
- **submissions** - Company metadata, JSONB arrays for tickers/exchanges/filings/addresses. PK: (cik, ingest_date). Used to skip already-ingested CIKs on re-runs.
- **companyfacts_metadata** - Per-company facts metadata (entity name, ingest date)
- **companyfacts_facts** - Normalized financial metrics (no label/description; join via view)
- **metric_metadata** - Canonical metric reference (taxonomy + metric_name PK, label, description, abbreviation, category)
- **ticker_prices_daily** - Daily OHLCV by (ticker, price_date); source (e.g. yahoo), fetched_at. For cheapness/valuation.

### Views
- **companyfacts_facts_full** - Facts joined with metric_metadata (label, description, abbreviation, category)
- **companyfacts_facts_with_abbrev** - Alias for companyfacts_facts_full
- **submissions_ticker_mapping** - Expands JSONB ticker/exchange arrays to rows; used by fetch_ticker_prices to select tickers

## Migration Tracking

Applied migrations are tracked in `sec_raw.schema_migrations`:
- Migration name and MD5 checksum
- Execution timestamp and duration
- Success/failure status with error messages

## Migration File Format

Files must follow the naming convention:
```
YYYYMMDDHHMM__description.sql
```

Migrations run in chronological order by filename prefix. The HHMM component ensures correct ordering when multiple migrations are created on the same day.

## Rollback

To rollback a migration, provide its name (without `.sql`):
```bash
python deploy_migrations.py --rollback 202512221000__create_submissions
```

If a matching `__rollback.sql` file exists in the migrations directory, its SQL is executed before removing the tracking record. Otherwise only the tracking record is removed and you must manually revert schema changes.
