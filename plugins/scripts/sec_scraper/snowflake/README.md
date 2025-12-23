# Snowflake Migration Deployment

This directory contains scripts to deploy SEC scraper migrations to Snowflake.
The migration system tracks which migrations have been executed to prevent re-running them.

## Prerequisites

1. Install `snowflake-connector-python`:
   ```bash
   pip install snowflake-connector-python
   ```

2. Configure Snowflake connection (see Configuration below)

## Configuration

You can configure Snowflake connection in two ways:

### Option 1: Environment Variables (Recommended)

Set these environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="YOUR_DATABASE"
export SNOWFLAKE_SCHEMA="sec_raw"  # Optional, defaults to sec_raw
export SNOWFLAKE_ROLE="ACCOUNTADMIN"  # Optional
```

### Option 2: Config File (Recommended)

1. Copy the example config to the `config/` folder:
   ```bash
   cp config/snowflake.json.example config/snowflake.json
   ```

2. Edit `config/snowflake.json` with your Snowflake credentials

3. The script will automatically find `config/snowflake.json` (or use `--config` to specify a different path)

**Note:** `config/snowflake.json` is gitignored - never commit credentials! The `.example` file is safe to commit.

## Migration Files

Migration files are located in `plugins/scripts/sec_scraper/snowflake/migrations/` and follow the naming pattern:
- `YYYYMMDD__description.sql` (e.g., `20251222__create_submissions.sql`)

Migrations are executed in chronological order (by date prefix, then filename).

## Usage

### From Local Machine

```bash
# Dry run (see what would be executed)
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --dry-run

# Deploy pending migrations to default schema (sec_raw)
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py

# Deploy to custom schema
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --schema my_schema

# Use config file (defaults to config/snowflake.json)
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py

# Or specify a different config file
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --config /path/to/config.json

# Verbose output
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --verbose
```

### From Airflow Container

```bash
# Enter the container
docker compose exec airflow-scheduler bash

# Set environment variables (or use Airflow connections)
export SNOWFLAKE_ACCOUNT="..."
export SNOWFLAKE_USER="..."
# ... etc

# Run deployment
python /opt/airflow/plugins/scripts/sec_scraper/snowflake/deploy_migrations.py
```

### Using Airflow Connections (Recommended for Production)

1. Create a Snowflake connection in Airflow UI:
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Fill in account, user, password, warehouse, database, schema

2. Modify the script to use Airflow's connection (or use Airflow's SnowflakeHook)

## Migration Tracking

The migration system:
- Tracks executed migrations in `{schema}.schema_migrations` table
- Prevents re-running migrations that have already been executed
- Detects modified migrations (checksum changes) and re-runs them with a warning
- Records execution time, success/failure, and error messages
- Each migration is identified by filename and checksum

**Note:** The old `deploy_schemas.py` script is deprecated. Use `deploy_migrations.py` instead.

## Migration Files

Migration files are in `plugins/scripts/sec_scraper/snowflake/migrations/` and are executed in chronological order:

1. `20251222__create_submissions.sql` - Company submissions metadata table
2. `20251222__create_companyfacts.sql` - Company facts tables (metadata + facts)
3. `20251222__create_us_gaap_metric_abbreviations.sql` - Metric abbreviation mapping table
4. `20251222__create_submissions_ticker_mapping.sql` - Ticker/exchange mapping view

## Schema Structure

The deployment creates:

- **Tables:**
  - `sec_raw.submissions` - Company submission metadata
  - `sec_raw.companyfacts_metadata` - Company facts metadata
  - `sec_raw.companyfacts_facts` - Individual metric values (normalized)
  - `sec_raw.companyfacts_variant` - Company facts as VARIANT (alternative)
  - `sec_raw.us_gaap_metric_abbreviations` - Metric abbreviation mappings

- **Views:**
  - `sec_raw.submissions_ticker_mapping` - Normalized ticker/exchange mapping
  - `sec_raw.companyfacts_facts_with_abbrev` - Facts with abbreviations joined

## Troubleshooting

### Connection Issues

- Verify your account format: `account.snowflakecomputing.com` (not just `account`)
- Check that your user has permissions to create schemas and tables
- Ensure warehouse is running: `ALTER WAREHOUSE COMPUTE_WH RESUME;`

### Permission Errors

- Ensure your role has `CREATE SCHEMA` and `CREATE TABLE` permissions
- You may need `USAGE` on the database: `GRANT USAGE ON DATABASE YOUR_DATABASE TO ROLE YOUR_ROLE;`

### SQL Errors

- Check that all SQL files are valid Snowflake DDL
- Some features may require specific Snowflake edition (e.g., VARIANT support)

## Security Notes

- Never commit `config.json` or credentials to git
- Use environment variables or Airflow connections in production
- Consider using key pair authentication instead of passwords
- Use least-privilege roles (not ACCOUNTADMIN) in production

