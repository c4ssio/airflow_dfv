# Snowflake Migration System

This directory contains SQL migration files and scripts to manage the Snowflake schema for the SEC scraper project.

## Files

- `migrations/` - SQL migration files (YYYYMMDDHHMM__description.sql format)
- `deploy_migrations.py` - Main script to deploy migrations (supports one-at-a-time options)
- `cleanup_schema.py` - Script to drop all objects in the schema
- `*.sh` - Convenience shell scripts for common operations
- `config.example.json` - Example configuration file
- `SYNTAX.md` - Quick reference for all commands

## Setup

1. **Create configuration file:**
   ```bash
   cp config.example.json ../../../../config/snowflake.json
   # Edit config/snowflake.json with your Snowflake credentials
   ```

2. **Set up virtual environment (if not already done):**
   ```bash
   ../../../../scripts/setup_venv.sh
   ```

## Quick Start Scripts

### Cleanup Schema (Drop Everything)

```bash
# Dry run to preview
./cleanup_schema.sh --dry-run

# Actually clean up (requires confirmation)
./cleanup_schema.sh
```

### Run All Migrations

```bash
# Dry run to preview
./run_migrations.sh --dry-run

# Run all pending migrations
./run_migrations.sh
```


## Usage (Python Scripts Directly)

### Deploy All Pending Migrations

```bash
# Using the venv helper script
../../../../scripts/run_with_venv.sh python deploy_migrations.py

# Or activate venv manually
source ../../../../.venv/bin/activate
python deploy_migrations.py
```

### Dry Run (Preview Changes)

```bash
python deploy_migrations.py --dry-run
```

### Rollback a Specific Migration

Rollback a migration by dropping the objects it created and removing its tracking record:

```bash
python deploy_migrations.py --rollback 202512221000__create_submissions.sql
```

**Note:** Rollback automatically extracts object names (tables/views) from the migration SQL and drops them. It will:
1. Drop views first (they may depend on tables)
2. Drop tables
3. Remove the migration record from `schema_migrations` table

### Clean Up Entire Schema

**WARNING:** This will delete ALL tables and views in the schema!

```bash
# Dry run first to see what will be deleted
python cleanup_schema.py --dry-run

# Actually clean up (requires confirmation)
python cleanup_schema.py
```

### Re-run All Migrations from Scratch

If you need to start fresh:

```bash
# 1. Clean up everything
./cleanup_schema.sh

# 2. Re-run all migrations
./run_migrations.sh
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
python deploy_migrations.py --rollback-one
```

This will:
- Find the most recently executed migration
- Ask for confirmation
- Drop all objects created by that migration
- Remove the migration record

### Rolling Back a Specific Migration

```bash
python deploy_migrations.py --rollback MIGRATION_NAME.sql
```

When rolling back a migration:
- Objects are dropped in reverse order (views before tables)
- The migration record is removed from tracking
- If objects don't exist, warnings are logged but execution continues
- Rollback does NOT automatically rollback dependent migrations

**Important:** Rollback extracts object names from `CREATE TABLE` and `CREATE VIEW` statements. If your migration creates objects with different patterns, you may need to manually drop them.

## Configuration

Configuration can be provided via:
1. JSON config file (default: `../../../../config/snowflake.json`)
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
2. Rollback the failed migration: `python deploy_migrations.py --rollback MIGRATION_NAME.sql`
3. Re-run migrations: `./run_migrations.sh`

### Objects Not Created

If objects aren't created despite migration showing success:
1. Check the `schema_migrations` table for execution records
2. Verify SQL statements are being split correctly (check logs)
3. Try rolling back and re-running: `python deploy_migrations.py --rollback MIGRATION_NAME.sql && ./run_migrations.sh`

### Schema Out of Sync

If your schema is out of sync:
1. Clean up everything: `./cleanup_schema.sh`
2. Re-run all migrations: `./run_migrations.sh`
