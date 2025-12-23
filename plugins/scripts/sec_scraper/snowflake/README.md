# Snowflake Schema Deployment

This directory contains scripts to deploy SEC scraper schemas to Snowflake.

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

### Option 2: Config File

1. Copy `config.example.json` to `config.json`:
   ```bash
   cp config.example.json config.json
   ```

2. Edit `config.json` with your Snowflake credentials

3. Run with `--config config.json`

**Note:** `config.json` is gitignored - never commit credentials!

## Usage

### From Local Machine

```bash
# Dry run (see what would be executed)
python plugins/scripts/sec_scraper/snowflake/deploy_schemas.py --dry-run

# Deploy to default schema (sec_raw)
python plugins/scripts/sec_scraper/snowflake/deploy_schemas.py

# Deploy to custom schema
python plugins/scripts/sec_scraper/snowflake/deploy_schemas.py --schema my_schema

# Use config file
python plugins/scripts/sec_scraper/snowflake/deploy_schemas.py --config config.json

# Verbose output
python plugins/scripts/sec_scraper/snowflake/deploy_schemas.py --verbose
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
python /opt/airflow/plugins/scripts/sec_scraper/snowflake/deploy_schemas.py
```

### Using Airflow Connections (Recommended for Production)

1. Create a Snowflake connection in Airflow UI:
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Fill in account, user, password, warehouse, database, schema

2. Modify the script to use Airflow's connection (or use Airflow's SnowflakeHook)

## SQL Files Deployed

The script deploys SQL files in this order:

1. `submissions.sql` - Company submissions metadata table
2. `companyfacts.sql` - Company facts tables (metadata + facts)
3. `us_gaap_metric_abbreviations.sql` - Metric abbreviation mapping table
4. `submissions_ticker_mapping.sql` - Ticker/exchange mapping view

All files are read from `plugins/scripts/sec_scraper/schemas/sec_raw/` directory (relative to the script).

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

