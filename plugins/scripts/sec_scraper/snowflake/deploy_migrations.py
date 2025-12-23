#!/usr/bin/env python3
"""
Deploy SEC scraper migrations to Snowflake.

This script applies SQL migration files to Snowflake in chronological order.
It tracks which migrations have been executed to avoid re-running them.

Usage:
    python deploy_migrations.py [--dry-run] [--schema SCHEMA_NAME]

Environment variables (or config file):
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA (default: sec_raw)
    SNOWFLAKE_ROLE (optional)
"""

import argparse
import hashlib
import logging
import os
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
                    {'NULL' if error_message is None else f"'{error_message.replace("'", "''")}'"} AS error_message,
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
        Parse migration filename: YYYYMMDD__description.sql
        Returns: (date_str, description)
        """
        match = re.match(r"^(\d{8})__(.+)\.sql$", filename)
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

        # Sort by date, then by filename
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
                    if not statement or statement.startswith("--"):
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
    """Load configuration from file or environment variables."""
    config = {}

    # Try to load from config file first
    if config_file and config_file.exists():
        import json

        with open(config_file, "r") as f:
            config = json.load(f)
        logger.info(f"Loaded config from {config_file}")

    # Environment variables override config file
    config["account"] = os.environ.get("SNOWFLAKE_ACCOUNT") or config.get("account")
    config["user"] = os.environ.get("SNOWFLAKE_USER") or config.get("user")
    config["password"] = os.environ.get("SNOWFLAKE_PASSWORD") or config.get("password")
    config["warehouse"] = os.environ.get("SNOWFLAKE_WAREHOUSE") or config.get("warehouse")
    config["database"] = os.environ.get("SNOWFLAKE_DATABASE") or config.get("database")
    config["schema"] = os.environ.get("SNOWFLAKE_SCHEMA", "sec_raw") or config.get("schema", "sec_raw")
    config["role"] = os.environ.get("SNOWFLAKE_ROLE") or config.get("role")

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
        default=Path(__file__).parent.parent.parent.parent.parent / "config" / "snowflake.json",
        help="Path to JSON config file (default: ../../../../config/snowflake.json)",
    )
    parser.add_argument("--schema", default="sec_raw", help="Snowflake schema name (default: sec_raw)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (don't execute SQL)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

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
        logger.error("Set via environment variables or --config file")
        sys.exit(1)

    # Check migrations directory
    if not args.migrations_dir.exists():
        logger.error(f"Migrations directory not found: {args.migrations_dir}")
        sys.exit(1)

    # Deploy
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
        migrator.deploy(args.migrations_dir)
    except Exception as e:
        logger.error(f"Migration deployment failed: {e}")
        sys.exit(1)
    finally:
        migrator.close()


if __name__ == "__main__":
    main()

