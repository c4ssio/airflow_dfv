#!/usr/bin/env python3
"""
Deploy SEC scraper migrations to Snowflake.

This script applies SQL migration files to Snowflake in chronological order.
It tracks which migrations have been executed to avoid re-running them.

Usage:
    python deploy_migrations.py [--dry-run] [--schema SCHEMA_NAME]
    python deploy_migrations.py --migrate-one [--dry-run] [--schema SCHEMA_NAME]
    python deploy_migrations.py --rollback MIGRATION_NAME [--schema SCHEMA_NAME]
    python deploy_migrations.py --rollback-one [--dry-run] [--schema SCHEMA_NAME]

Configuration:
    Config file: config/snowflake.yaml (default)
    Use --config to specify a different config file path
"""

import argparse
import hashlib
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import snowflake.connector
except ImportError:
    print("Error: snowflake-connector-python not installed.")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)

logger = logging.getLogger(__name__)


class MigrationTracker:
    """Track and manage database migrations."""

    def __init__(self, conn, schema: str):
        self.conn = conn
        self.schema = schema
        self.migrations_table = f"{schema}.schema_migrations"

    def ensure_migrations_table(self):
        """Create migrations tracking table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.migrations_table} (
            migration_name STRING NOT NULL PRIMARY KEY,
            checksum STRING NOT NULL,
            executed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            executed_by STRING,
            execution_time_ms INTEGER,
            success BOOLEAN NOT NULL DEFAULT TRUE,
            error_message STRING
        )
        COMMENT = 'Tracks executed database migrations to prevent re-running';
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(create_table_sql)
            cursor.close()
            logger.debug(f"Ensured migrations table exists: {self.migrations_table}")
        except Exception as e:
            cursor.close()
            logger.error(f"Failed to create migrations table: {e}")
            raise

    def get_executed_migrations(self) -> dict:
        """Get all executed migrations with their checksums."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT migration_name, checksum, executed_at, success
                FROM {self.migrations_table}
                WHERE success = TRUE
                ORDER BY executed_at
                """
            )
            results = cursor.fetchall()
            cursor.close()

            migrations = {}
            for row in results:
                migrations[row[0]] = {
                    "checksum": row[1],
                    "executed_at": row[2],
                    "success": row[3],
                }
            return migrations
        except Exception as e:
            cursor.close()
            logger.error(f"Failed to get executed migrations: {e}")
            raise

    def record_migration(
        self,
        migration_name: str,
        checksum: str,
        execution_time_ms: int,
        success: bool = True,
        error_message: Optional[str] = None,
    ):
        """Record a migration execution."""
        cursor = self.conn.cursor()
        try:
            # Get current user
            cursor.execute("SELECT CURRENT_USER()")
            current_user = cursor.fetchone()[0]

            # Use MERGE to handle both insert and update
            merge_sql = f"""
            MERGE INTO {self.migrations_table} AS target
            USING (
                SELECT 
                    '{migration_name}' AS migration_name,
                    '{checksum}' AS checksum,
                    {execution_time_ms} AS execution_time_ms,
                    {success} AS success,
                    {'NULL' if error_message is None else f"'{error_message.replace(chr(39), chr(39)+chr(39))}'"} AS error_message,
                    '{current_user}' AS executed_by
            ) AS source
            ON target.migration_name = source.migration_name
            WHEN MATCHED THEN
                UPDATE SET
                    checksum = source.checksum,
                    executed_at = CURRENT_TIMESTAMP(),
                    execution_time_ms = source.execution_time_ms,
                    success = source.success,
                    error_message = source.error_message,
                    executed_by = source.executed_by
            WHEN NOT MATCHED THEN
                INSERT (migration_name, checksum, execution_time_ms, success, error_message, executed_by)
                VALUES (source.migration_name, source.checksum, source.execution_time_ms, 
                        source.success, source.error_message, source.executed_by)
            """
            cursor.execute(merge_sql)
            cursor.close()
            logger.debug(f"Recorded migration: {migration_name}")
        except Exception as e:
            cursor.close()
            logger.error(f"Failed to record migration {migration_name}: {e}")
            raise


class SnowflakeMigrator:
    """Deploy SQL migrations to Snowflake with tracking."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str = "sec_raw",
        role: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.dry_run = dry_run
        self.conn = None
        self.tracker = None

    def connect(self):
        """Establish connection to Snowflake."""
        if self.dry_run:
            logger.info("DRY RUN: Would connect to Snowflake")
            return

        try:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
            )
            self.tracker = MigrationTracker(self.conn, self.schema)
            logger.info(f"Connected to Snowflake: {self.account}/{self.database}/{self.schema}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def close(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")

    def calculate_checksum(self, content: str) -> str:
        """Calculate SHA256 checksum of SQL content."""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def parse_migration_filename(self, filename: str) -> Tuple[Optional[str], str]:
        """
        Parse migration filename: YYYYMMDDHHMM__description.sql
        Returns: (date_str, description)
        """
        match = re.match(r"^(\d{12})__(.+)\.sql$", filename)
        if match:
            return match.group(1), match.group(2)
        return None, filename

    def find_migrations(self, migrations_dir: Path) -> List[Tuple[Path, str, str]]:
        """
        Find all migration files and return sorted list of (path, date, name).
        Migrations are sorted by date, then by filename.
        """
        migrations = []
        for sql_file in sorted(migrations_dir.glob("*.sql")):
            date_str, description = self.parse_migration_filename(sql_file.name)
            if date_str:
                migrations.append((sql_file, date_str, sql_file.name))
            else:
                logger.warning(f"Migration file doesn't match naming pattern: {sql_file.name}")

        # Sort by date (which includes timestamp if present), then by filename
        # This ensures migrations run in chronological order
        migrations.sort(key=lambda x: (x[1], x[2]))
        return migrations

    def read_sql_file(self, filepath: Path) -> str:
        """Read SQL file and replace schema placeholder."""
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()

        # Replace schema placeholder if present
        sql = sql.replace("sec_raw.", f"{self.schema}.")
        # Also handle CREATE SCHEMA statements
        sql = sql.replace("CREATE SCHEMA sec_raw", f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        sql = sql.replace("USE SCHEMA sec_raw", f"USE SCHEMA {self.schema}")

        return sql

    def execute_sql(self, sql: str, description: str = ""):
        """Execute SQL statement."""
        if self.dry_run:
            logger.info(f"DRY RUN: Would execute SQL ({description})")
            logger.debug(f"SQL:\n{sql[:500]}...")
            return

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            cursor.close()
            logger.info(f"✓ Executed: {description}")
        except Exception as e:
            logger.error(f"✗ Failed to execute {description}: {e}")
            raise

    def get_latest_migration(self) -> Optional[str]:
        """Get the most recent migration name from the tracking table."""
        if self.dry_run:
            return None

        self.tracker.ensure_migrations_table()
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT migration_name, executed_at
                FROM {self.tracker.migrations_table}
                WHERE success = TRUE
                ORDER BY executed_at DESC
                LIMIT 1
            """
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        finally:
            cursor.close()

    def get_next_pending_migration(self, migrations_dir: Path) -> Optional[Tuple[Path, str, str]]:
        """Get the next pending migration that hasn't been executed yet."""
        executed = {}
        if not self.dry_run and self.tracker:
            executed = self.tracker.get_executed_migrations()
        migrations = self.find_migrations(migrations_dir)

        for filepath, date_str, migration_name in migrations:
            if migration_name not in executed:
                return (filepath, date_str, migration_name)
        return None

    def rollback_migration(self, migration_name: str):
        """Rollback a specific migration by dropping its objects."""
        if self.dry_run:
            logger.info(f"DRY RUN: Would rollback migration {migration_name}")
            return

        # Ensure migrations table exists
        self.tracker.ensure_migrations_table()

        # Get executed migrations
        executed = self.tracker.get_executed_migrations()
        if migration_name not in executed:
            logger.warning(f"Migration {migration_name} not found in executed migrations")
            logger.info("Available migrations:")
            for name in executed.keys():
                logger.info(f"  - {name}")
            return

        logger.info(f"Rolling back migration: {migration_name}")

        # Find the migration file
        migrations_dir = Path(__file__).parent / "migrations"
        migration_file = migrations_dir / migration_name
        if not migration_file.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return

        # Read and parse the migration to extract object names
        sql_content = self.read_sql_file(migration_file)
        objects_to_drop = self._extract_objects_from_sql(sql_content)

        # Drop objects in reverse order (views first, then tables)
        cursor = self.conn.cursor()
        try:
            for obj_type, obj_name in reversed(objects_to_drop):
                full_name = f"{self.schema}.{obj_name}"
                if obj_type == "VIEW":
                    drop_sql = f"DROP VIEW IF EXISTS {full_name}"
                elif obj_type == "TABLE":
                    drop_sql = f"DROP TABLE IF EXISTS {full_name}"
                else:
                    continue

                try:
                    cursor.execute(drop_sql)
                    logger.info(f"✓ Dropped {obj_type.lower()} {full_name}")
                except Exception as e:
                    logger.warning(f"⚠ Failed to drop {obj_type.lower()} {full_name}: {e}")

            # Remove migration record
            cursor.execute(
                f"DELETE FROM {self.tracker.migrations_table} WHERE migration_name = '{migration_name}'"
            )
            logger.info(f"✓ Removed migration record for {migration_name}")

        finally:
            cursor.close()

    def _extract_objects_from_sql(self, sql: str) -> List[Tuple[str, str]]:
        """
        Extract CREATE TABLE and CREATE VIEW object names from SQL.
        Returns list of (object_type, object_name) tuples.
        """
        import re

        objects = []

        # Find CREATE TABLE statements
        table_pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:\w+\.)?(\w+)"
        for match in re.finditer(table_pattern, sql, re.IGNORECASE):
            objects.append(("TABLE", match.group(1).upper()))

        # Find CREATE VIEW statements
        view_pattern = r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:\w+\.)?(\w+)"
        for match in re.finditer(view_pattern, sql, re.IGNORECASE):
            objects.append(("VIEW", match.group(1).upper()))

        return objects

    def deploy_one(self, migrations_dir: Path):
        """Deploy the next pending migration only."""
        logger.info(f"Deploying next pending migration to Snowflake schema: {self.schema}")

        # Ensure schema exists
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        self.execute_sql(create_schema_sql, f"Create schema {self.schema}")

        # Ensure migrations table exists
        if not self.dry_run:
            self.tracker.ensure_migrations_table()

        # Get executed migrations
        executed = {}
        if not self.dry_run and self.tracker:
            executed = self.tracker.get_executed_migrations()

        # Find the next pending migration
        next_migration = self.get_next_pending_migration(migrations_dir)
        if not next_migration:
            logger.info("✓ All migrations are up to date")
            return

        filepath, date_str, migration_name = next_migration

        # Check if already executed (shouldn't happen, but check anyway)
        if migration_name in executed:
            existing_checksum = executed[migration_name]["checksum"]
            sql_content = self.read_sql_file(filepath)
            current_checksum = self.calculate_checksum(sql_content)

            if current_checksum == existing_checksum:
                logger.info(f"⏭ Skipping {migration_name} (already executed)")
                return
            else:
                logger.warning(
                    f"⚠ Migration {migration_name} was modified (checksum changed). "
                    f"Re-running..."
                )

        # Execute the migration
        logger.info(f"▶ Executing migration: {migration_name}")

        sql_content = self.read_sql_file(filepath)
        checksum = self.calculate_checksum(sql_content)

        # Split SQL file by semicolons
        statements = self._split_sql_statements(sql_content)

        start_time = datetime.now()
        success = True
        error_message = None

        try:
            for i, statement in enumerate(statements, 1):
                statement = statement.strip()
                if not statement:
                    continue

                # Skip pure comment lines, but strip leading comments from statements
                if statement.startswith("--") and "\n" not in statement:
                    continue

                # Remove leading comment lines from the statement
                lines = statement.split('\n')
                cleaned_lines = []
                for line in lines:
                    stripped = line.strip()
                    if stripped and not stripped.startswith("--"):
                        cleaned_lines.append(line)
                    elif not stripped:
                        # Keep empty lines for formatting
                        cleaned_lines.append(line)
                statement = '\n'.join(cleaned_lines).strip()

                if not statement:
                    continue

                desc = f"{migration_name} (statement {i}/{len(statements)})"
                self.execute_sql(statement, desc)

            execution_time = (datetime.now() - start_time).total_seconds() * 1000

            # Record successful migration
            if not self.dry_run:
                self.tracker.record_migration(
                    migration_name=migration_name,
                    checksum=checksum,
                    execution_time_ms=int(execution_time),
                    success=True,
                    error_message=None,
                )
            logger.info(f"✓ Completed migration: {migration_name}")

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            error_message = str(e)
            if not self.dry_run:
                self.tracker.record_migration(
                    migration_name=migration_name,
                    checksum=checksum,
                    execution_time_ms=int(execution_time),
                    success=False,
                    error_message=error_message,
                )
            logger.error(f"✗ Migration failed: {migration_name} - {error_message}")
            raise

    def deploy(self, migrations_dir: Path):
        """Deploy all pending migrations in order."""
        logger.info(f"Deploying migrations to Snowflake schema: {self.schema}")

        # Ensure schema exists
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        self.execute_sql(create_schema_sql, f"Create schema {self.schema}")

        # Ensure migrations table exists
        if not self.dry_run:
            self.tracker.ensure_migrations_table()

        # Get executed migrations
        executed = {}
        if not self.dry_run:
            executed = self.tracker.get_executed_migrations()

        # Find all migration files
        migrations = self.find_migrations(migrations_dir)
        logger.info(f"Found {len(migrations)} migration files")

        # Process each migration
        pending_count = 0
        for filepath, date_str, migration_name in migrations:
            # Check if already executed
            if migration_name in executed:
                existing_checksum = executed[migration_name]["checksum"]
                sql_content = self.read_sql_file(filepath)
                current_checksum = self.calculate_checksum(sql_content)

                if current_checksum == existing_checksum:
                    logger.info(f"⏭ Skipping {migration_name} (already executed)")
                    continue
                else:
                    logger.warning(
                        f"⚠ Migration {migration_name} was modified (checksum changed). "
                        f"Re-running..."
                    )

            # New or modified migration - execute it
            logger.info(f"▶ Executing migration: {migration_name}")
            pending_count += 1

            sql_content = self.read_sql_file(filepath)
            checksum = self.calculate_checksum(sql_content)

            # Split SQL file by semicolons
            statements = self._split_sql_statements(sql_content)

            start_time = datetime.now()
            success = True
            error_message = None

            try:
                for i, statement in enumerate(statements, 1):
                    statement = statement.strip()
                    if not statement:
                        continue
                    
                    # Skip pure comment lines, but strip leading comments from statements
                    if statement.startswith("--") and "\n" not in statement:
                        continue
                    
                    # Remove leading comment lines from the statement
                    lines = statement.split('\n')
                    cleaned_lines = []
                    for line in lines:
                        stripped = line.strip()
                        if stripped and not stripped.startswith("--"):
                            cleaned_lines.append(line)
                        elif not stripped:
                            # Keep empty lines for formatting
                            cleaned_lines.append(line)
                    statement = '\n'.join(cleaned_lines).strip()
                    
                    if not statement:
                        continue

                    desc = f"{migration_name} (statement {i}/{len(statements)})"
                    self.execute_sql(statement, desc)

                execution_time = (datetime.now() - start_time).total_seconds() * 1000

                # Record successful migration
                if not self.dry_run:
                    self.tracker.record_migration(
                        migration_name=migration_name,
                        checksum=checksum,
                        execution_time_ms=int(execution_time),
                        success=True,
                    )
                logger.info(f"✓ Completed migration: {migration_name}")

            except Exception as e:
                success = False
                error_message = str(e)
                execution_time = (datetime.now() - start_time).total_seconds() * 1000

                # Record failed migration
                if not self.dry_run:
                    self.tracker.record_migration(
                        migration_name=migration_name,
                        checksum=checksum,
                        execution_time_ms=int(execution_time),
                        success=False,
                        error_message=error_message,
                    )
                logger.error(f"✗ Migration failed: {migration_name} - {error_message}")
                raise

        if pending_count == 0:
            logger.info("✓ All migrations are up to date")
        else:
            logger.info(f"✓ Deployed {pending_count} migration(s)")

    @staticmethod
    def _split_sql_statements(sql: str) -> List[str]:
        """Split SQL into individual statements, handling semicolons in strings/comments."""
        statements = []
        current = []
        in_string = False
        string_char = None
        in_comment = False

        i = 0
        while i < len(sql):
            char = sql[i]

            # Handle string literals
            if char in ("'", '"') and not in_comment:
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char and (i == 0 or sql[i - 1] != "\\"):
                    in_string = False
                    string_char = None

            # Handle comments
            elif not in_string:
                if i < len(sql) - 1 and sql[i : i + 2] == "--":
                    in_comment = True
                elif in_comment and char == "\n":
                    in_comment = False

            # Split on semicolon (outside strings/comments)
            if char == ";" and not in_string and not in_comment:
                stmt = "".join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)

            i += 1

        # Add final statement if any
        final_stmt = "".join(current).strip()
        if final_stmt:
            statements.append(final_stmt)

        return statements


def load_config(config_file: Optional[Path] = None) -> dict:
    """Load configuration from YAML file."""
    if not config_file or not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    import yaml

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded config from {config_file}")

    # Set default schema if not provided
    if "schema" not in config:
        config["schema"] = "sec_raw"

    return config


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Deploy SEC scraper migrations to Snowflake")
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=Path(__file__).parent / "migrations",
        help="Directory containing migration files (default: ./migrations)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent.parent.parent.parent.parent / "config" / "snowflake.yaml",
        help="Path to YAML config file (default: ../../../../config/snowflake.yaml)",
    )
    parser.add_argument("--schema", default="sec_raw", help="Snowflake schema name (default: sec_raw)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (don't execute SQL)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument(
        "--rollback",
        type=str,
        metavar="MIGRATION_NAME",
        help="Rollback a specific migration (e.g., 202512221000__create_submissions.sql)",
    )
    parser.add_argument(
        "--rollback-one",
        action="store_true",
        help="Rollback the most recent migration (one at a time)",
    )
    parser.add_argument(
        "--migrate-one",
        action="store_true",
        help="Run only the next pending migration (one at a time)",
    )

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Load configuration
    config = load_config(args.config)
    config["schema"] = args.schema

    # Validate required config
    required = ["account", "user", "password", "warehouse", "database"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        logger.error(f"Missing required configuration: {', '.join(missing)}")
        logger.error(f"Please check your config file: {args.config}")
        sys.exit(1)

    # Check migrations directory
    if not args.migrations_dir.exists():
        logger.error(f"Migrations directory not found: {args.migrations_dir}")
        sys.exit(1)

    # Deploy or rollback
    migrator = SnowflakeMigrator(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        role=config.get("role"),
        dry_run=args.dry_run,
    )

    try:
        migrator.connect()

        if args.rollback:
            # Rollback specific migration
            migrator.rollback_migration(args.rollback)
        elif args.rollback_one:
            # Rollback the most recent migration
            latest = migrator.get_latest_migration()
            if not latest:
                logger.warning("No executed migrations found")
                sys.exit(0)

            logger.info(f"Most recent migration: {latest}")
            if not args.dry_run:
                response = input(f"Rollback migration {latest}? (yes/no): ")
                if response.lower() != "yes":
                    logger.info("Aborted.")
                    sys.exit(0)

            migrator.rollback_migration(latest)
        elif args.migrate_one:
            # Deploy only the next pending migration
            migrator.deploy_one(args.migrations_dir)
        else:
            # Deploy all pending migrations
            migrator.deploy(args.migrations_dir)

    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)
    finally:
        migrator.close()


if __name__ == "__main__":
    main()

