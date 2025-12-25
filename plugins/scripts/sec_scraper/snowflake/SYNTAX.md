# Migration Script Syntax Reference

## Quick Commands

### Cleanup Schema (Drop Everything)
```bash
./cleanup_schema.sh --dry-run    # Preview what will be deleted
./cleanup_schema.sh              # Actually delete (requires confirmation)
```

### Run All Migrations
```bash
./run_migrations.sh --dry-run    # Preview what will run
./run_migrations.sh              # Run all pending migrations
```

## Deploy Migrations Options

### Run All Pending Migrations (Default)
```bash
python deploy_migrations.py
python deploy_migrations.py --dry-run
```

### Run One Migration at a Time (Migrate Up)
```bash
# Preview the next migration
python deploy_migrations.py --migrate-one --dry-run

# Run only the next pending migration
python deploy_migrations.py --migrate-one
```

### Rollback One Migration at a Time (Migrate Down)
```bash
# Preview what would be rolled back
python deploy_migrations.py --rollback-one --dry-run

# Rollback the most recent migration (requires confirmation)
python deploy_migrations.py --rollback-one
```

### Rollback Specific Migration
```bash
python deploy_migrations.py --rollback 202512221000__create_submissions.sql
```

## Common Workflows

### Complete Reset
```bash
./cleanup_schema.sh      # Drop everything
./run_migrations.sh      # Re-run all migrations
```

### Step-by-Step Migration
```bash
# Run migrations one at a time
python deploy_migrations.py --migrate-one
python deploy_migrations.py --migrate-one
python deploy_migrations.py --migrate-one

# Rollback one at a time if needed
python deploy_migrations.py --rollback-one
```

### Fix a Failed Migration
```bash
# 1. Rollback the failed migration
python deploy_migrations.py --rollback-one

# 2. Fix the migration file

# 3. Re-run it
python deploy_migrations.py --migrate-one
```

## Options Reference

- `--migrate-one` - Run only the next pending migration (one at a time)
- `--rollback-one` - Rollback the most recent migration (one at a time)
- `--rollback MIGRATION_NAME` - Rollback a specific migration by name
- `--dry-run` - Preview changes without executing
- `--schema SCHEMA_NAME` - Specify schema (default: sec_raw)
- `--config PATH` - Specify config file path
- `--verbose` or `-v` - Verbose logging

