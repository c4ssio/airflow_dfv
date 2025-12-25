# Snowflake Migration System

This directory contains SQL migration files and scripts to manage the Snowflake schema for the SEC scraper project.

## Files

- `migrations/` - SQL migration files (YYYYMMDDHHMM__description.sql format)
- `deploy_migrations.py` - Main script to deploy migrations (supports one-at-a-time options)
- `cleanup_schema.py` - Script to drop all objects in the schema
- `*.sh` - Convenience shell scripts for common operations
- `config.example.json` - Example configuration file

## Setup

**Note:** All commands assume you are in the `airflow_dfv` project root directory.

1. **Set up virtual environment (required):**
   ```bash
   ./scripts/setup_venv.sh
   ```

2. **Create configuration file:**
   ```bash
   cp plugins/scripts/sec_scraper/snowflake/config.example.json config/snowflake.json
   # Edit config/snowflake.json with your Snowflake credentials
   ```

**Note:** All Python scripts must be run with the virtual environment activated. The shell scripts handle this automatically.

## Quick Commands

### Cleanup Schema (Drop Everything)

```bash
# Dry run to preview
./plugins/scripts/sec_scraper/snowflake/cleanup_schema.sh --dry-run

# Actually clean up (requires confirmation)
./plugins/scripts/sec_scraper/snowflake/cleanup_schema.sh
```

### Run All Migrations

```bash
# Dry run to preview
./plugins/scripts/sec_scraper/snowflake/run_migrations.sh --dry-run

# Run all pending migrations
./plugins/scripts/sec_scraper/snowflake/run_migrations.sh
```

## Deploy Migrations Options

### Run All Pending Migrations (Default)

```bash
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --dry-run
```

### Run One Migration at a Time (Migrate Up)

```bash
# Preview the next migration
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one --dry-run

# Run only the next pending migration
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one
```

### Rollback One Migration at a Time (Migrate Down)

```bash
# Preview what would be rolled back
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback-one --dry-run

# Rollback the most recent migration (requires confirmation)
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback-one
```

### Rollback Specific Migration

```bash
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback 202512221000__create_submissions.sql
```

**Note:** Rollback automatically extracts object names (tables/views) from the migration SQL and drops them. It will:
1. Drop views first (they may depend on tables)
2. Drop tables
3. Remove the migration record from `schema_migrations` table

## Common Workflows

### Complete Reset

```bash
./plugins/scripts/sec_scraper/snowflake/cleanup_schema.sh      # Drop everything
./plugins/scripts/sec_scraper/snowflake/run_migrations.sh      # Re-run all migrations
```

### Step-by-Step Migration

```bash
# Run migrations one at a time
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one

# Rollback one at a time if needed
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback-one
```

### Fix a Failed Migration

```bash
# 1. Rollback the failed migration
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback-one

# 2. Fix the migration file

# 3. Re-run it
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --migrate-one
```

## Migration File Format

Migration files must follow this naming convention:
```
YYYYMMDDHHMM__description.sql
```

Or the older format (still supported):
```
YYYYMMDD__description.sql
```

Example: `202512221000__create_submissions.sql`

The migration system:
- Executes migrations in chronological order (by date/timestamp prefix)
- Tracks executed migrations in `schema_migrations` table
- Skips already-executed migrations (unless file content changed)
- Supports `CREATE OR REPLACE` statements for idempotency

**Note:** The `HHMM` (hours and minutes) format ensures migrations run in the exact order you specify, even if multiple migrations are created on the same day.

## Migration Tracking

The system creates a `schema_migrations` table to track:
- Migration name
- Execution timestamp
- SQL checksum (to detect file changes)
- Success/failure status

## Rollback Behavior

### Rolling Back One at a Time

```bash
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback-one
```

This will:
- Find the most recently executed migration
- Ask for confirmation
- Drop all objects created by that migration
- Remove the migration record

### Rolling Back a Specific Migration

```bash
python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback MIGRATION_NAME.sql
```

When rolling back a migration:
- Objects are dropped in reverse order (views before tables)
- The migration record is removed from tracking
- If objects don't exist, warnings are logged but execution continues
- Rollback does NOT automatically rollback dependent migrations

**Important:** Rollback extracts object names from `CREATE TABLE` and `CREATE VIEW` statements. If your migration creates objects with different patterns, you may need to manually drop them.

## Options Reference

- `--migrate-one` - Run only the next pending migration (one at a time)
- `--rollback-one` - Rollback the most recent migration (one at a time)
- `--rollback MIGRATION_NAME` - Rollback a specific migration by name
- `--dry-run` - Preview changes without executing
- `--schema SCHEMA_NAME` - Specify schema (default: sec_raw)
- `--config PATH` - Specify config file path
- `--verbose` or `-v` - Verbose logging

## Configuration

Configuration can be provided via:
1. JSON config file (default: `config/snowflake.json`)
2. Environment variables (override config file):
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_SCHEMA` (default: `sec_raw`)
   - `SNOWFLAKE_ROLE` (optional)

## Troubleshooting

### Migration Fails Partway Through

If a migration fails partway through:
1. Fix the migration file
2. Rollback the failed migration: `python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback MIGRATION_NAME.sql`
3. Re-run migrations: `./plugins/scripts/sec_scraper/snowflake/run_migrations.sh`

### Objects Not Created

If objects aren't created despite migration showing success:
1. Check the `schema_migrations` table for execution records
2. Verify SQL statements are being split correctly (check logs)
3. Try rolling back and re-running: `python plugins/scripts/sec_scraper/snowflake/deploy_migrations.py --rollback MIGRATION_NAME.sql && ./plugins/scripts/sec_scraper/snowflake/run_migrations.sh`

### Schema Out of Sync

If your schema is out of sync:
1. Clean up everything: `./plugins/scripts/sec_scraper/snowflake/cleanup_schema.sh`
2. Re-run all migrations: `./plugins/scripts/sec_scraper/snowflake/run_migrations.sh`
