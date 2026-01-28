#!/usr/bin/env python3
"""
PostgreSQL Migration Runner for SEC Data Schema

Runs SQL migrations in order, tracking which have been applied.
Supports rollback via --rollback flag.

Usage:
    python deploy_migrations.py [--dry-run] [--verbose] [--rollback MIGRATION_NAME]

Environment:
    POSTGRES_CONFIG_PATH: Path to postgres.yaml config file
"""

import argparse
import hashlib
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import sql
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load PostgreSQL configuration from YAML file."""
    if config_path is None:
        # Default paths to check
        default_paths = [
            "/opt/airflow/config/postgres.yaml",
            str(Path(__file__).parent.parent.parent.parent.parent / "config" / "postgres.yaml"),
        ]
        for path in default_paths:
            if os.path.exists(path):
                config_path = path
                break
        else:
            raise FileNotFoundError(
                f"PostgreSQL config not found. Checked: {default_paths}"
            )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    required_keys = ["host", "port", "database", "user", "password"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(missing)}")

    return config


def get_connection(config: Dict[str, Any]) -> psycopg2.extensions.connection:
    """Get a connection to PostgreSQL."""
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
    )
    conn.autocommit = False
    return conn


def ensure_database_exists(config: Dict[str, Any]) -> None:
    """Ensure the target database exists, create if not."""
    # Connect to default 'postgres' database to create our database
    admin_config = config.copy()
    admin_config["database"] = "postgres"

    try:
        conn = psycopg2.connect(
            host=admin_config["host"],
            port=admin_config["port"],
            database=admin_config["database"],
            user=admin_config["user"],
            password=admin_config["password"],
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (config["database"],)
        )
        if not cursor.fetchone():
            logger.info(f"Creating database: {config['database']}")
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(config["database"])
                )
            )

        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not check/create database: {e}")


class MigrationTracker:
    """Tracks which migrations have been applied."""

    def __init__(self, conn: psycopg2.extensions.connection, schema: str = "sec_raw"):
        self.conn = conn
        self.schema = schema
        self.migrations_table = f"{schema}.schema_migrations"
        self._ensure_tracking_table()

    def _ensure_tracking_table(self):
        """Create the migrations tracking table if it doesn't exist."""
        cursor = self.conn.cursor()
        try:
            # Ensure schema exists
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.schema)
                )
            )

            # Create migrations table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.migrations_table} (
                    migration_name TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms INTEGER,
                    success BOOLEAN DEFAULT TRUE,
                    error_message TEXT,
                    executed_by TEXT
                )
            """)
            self.conn.commit()
            logger.debug(f"Ensured tracking table exists: {self.migrations_table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create tracking table: {e}")
            raise
        finally:
            cursor.close()

    def get_applied_migrations(self) -> Dict[str, str]:
        """Get dict of applied migration names to checksums."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"SELECT migration_name, checksum FROM {self.migrations_table} WHERE success = TRUE"
            )
            return {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()

    def record_migration(
        self,
        migration_name: str,
        checksum: str,
        execution_time_ms: int,
        success: bool = True,
        error_message: Optional[str] = None,
    ):
        """Record a migration execution using parameterized queries."""
        cursor = self.conn.cursor()
        try:
            # Get current user
            cursor.execute("SELECT CURRENT_USER")
            current_user = cursor.fetchone()[0]

            # Check if migration already exists
            cursor.execute(
                f"SELECT COUNT(*) FROM {self.migrations_table} WHERE migration_name = %s",
                (migration_name,)
            )
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing record
                cursor.execute(
                    f"""
                    UPDATE {self.migrations_table}
                    SET checksum = %s,
                        executed_at = CURRENT_TIMESTAMP,
                        execution_time_ms = %s,
                        success = %s,
                        error_message = %s,
                        executed_by = %s
                    WHERE migration_name = %s
                    """,
                    (checksum, execution_time_ms, success, error_message, current_user, migration_name)
                )
            else:
                # Insert new record
                cursor.execute(
                    f"""
                    INSERT INTO {self.migrations_table}
                        (migration_name, checksum, execution_time_ms, success, error_message, executed_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (migration_name, checksum, execution_time_ms, success, error_message, current_user)
                )

            self.conn.commit()
            logger.debug(f"Recorded migration: {migration_name}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to record migration {migration_name}: {e}")
            raise
        finally:
            cursor.close()

    def remove_migration(self, migration_name: str):
        """Remove a migration record (for rollback)."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {self.migrations_table} WHERE migration_name = %s",
                (migration_name,)
            )
            self.conn.commit()
            logger.debug(f"Removed migration record: {migration_name}")
        finally:
            cursor.close()


class MigrationRunner:
    """Runs SQL migrations."""

    def __init__(
        self,
        config: Dict[str, Any],
        migrations_dir: Optional[str] = None,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.config = config
        self.schema = config.get("schema", "sec_raw")
        self.dry_run = dry_run
        self.verbose = verbose

        if migrations_dir is None:
            migrations_dir = str(Path(__file__).parent / "migrations")
        self.migrations_dir = Path(migrations_dir)

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        # Ensure database exists
        ensure_database_exists(config)

        self.conn = get_connection(config)
        self.tracker = MigrationTracker(self.conn, self.schema)

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def get_migration_files(self) -> List[Tuple[str, Path]]:
        """Get list of migration files sorted by name."""
        if not self.migrations_dir.exists():
            logger.warning(f"Migrations directory not found: {self.migrations_dir}")
            return []

        files = []
        for f in self.migrations_dir.glob("*.sql"):
            # Extract migration name (filename without extension)
            name = f.stem
            files.append((name, f))

        # Sort by name (which should start with timestamp)
        files.sort(key=lambda x: x[0])
        return files

    def compute_checksum(self, content: str) -> str:
        """Compute MD5 checksum of migration content."""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def run_migrations(self) -> Tuple[int, int, int]:
        """
        Run all pending migrations.

        Returns:
            Tuple of (applied, skipped, failed) counts
        """
        migration_files = self.get_migration_files()
        applied_migrations = self.tracker.get_applied_migrations()

        applied = 0
        skipped = 0
        failed = 0

        for name, path in migration_files:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            checksum = self.compute_checksum(content)

            # Check if already applied
            if name in applied_migrations:
                if applied_migrations[name] == checksum:
                    logger.debug(f"Skipping (already applied): {name}")
                    skipped += 1
                    continue
                else:
                    logger.warning(
                        f"Migration {name} has changed since last run. "
                        f"Old checksum: {applied_migrations[name]}, New: {checksum}"
                    )
                    # Still skip - don't re-run changed migrations automatically
                    skipped += 1
                    continue

            # Run the migration
            logger.info(f"{'[DRY-RUN] Would apply' if self.dry_run else 'Applying'}: {name}")

            if self.dry_run:
                applied += 1
                continue

            start_time = datetime.now()
            cursor = self.conn.cursor()
            try:
                cursor.execute(content)
                self.conn.commit()
                execution_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)

                self.tracker.record_migration(
                    migration_name=name,
                    checksum=checksum,
                    execution_time_ms=execution_time_ms,
                    success=True,
                )
                logger.info(f"Applied: {name} ({execution_time_ms}ms)")
                applied += 1
            except Exception as e:
                self.conn.rollback()
                execution_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                error_msg = str(e)[:500]

                self.tracker.record_migration(
                    migration_name=name,
                    checksum=checksum,
                    execution_time_ms=execution_time_ms,
                    success=False,
                    error_message=error_msg,
                )
                logger.error(f"Failed: {name} - {error_msg}")
                failed += 1
            finally:
                cursor.close()

        return applied, skipped, failed

    def rollback_migration(self, migration_name: str) -> bool:
        """
        Rollback a specific migration.

        Note: This only removes the migration record. Actual rollback SQL
        would need to be provided separately (e.g., in a down migration file).
        """
        logger.info(f"{'[DRY-RUN] Would rollback' if self.dry_run else 'Rolling back'}: {migration_name}")

        if self.dry_run:
            return True

        # Look for a rollback file
        rollback_path = self.migrations_dir / f"{migration_name}__rollback.sql"
        if rollback_path.exists():
            with open(rollback_path, "r", encoding="utf-8") as f:
                rollback_sql = f.read()

            cursor = self.conn.cursor()
            try:
                cursor.execute(rollback_sql)
                self.conn.commit()
                logger.info(f"Executed rollback SQL for: {migration_name}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Rollback SQL failed: {e}")
                return False
            finally:
                cursor.close()

        # Remove migration record
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {self.tracker.migrations_table} WHERE migration_name = %s",
                (migration_name,)
            )
            self.conn.commit()
            logger.info(f"Removed migration record: {migration_name}")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to remove migration record: {e}")
            return False
        finally:
            cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Run PostgreSQL migrations for SEC data")
    parser.add_argument("--config", help="Path to postgres.yaml config file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--rollback", metavar="MIGRATION", help="Rollback a specific migration")

    args = parser.parse_args()

    try:
        config = get_config(args.config)
        runner = MigrationRunner(
            config=config,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        if args.rollback:
            success = runner.rollback_migration(args.rollback)
            runner.close()
            sys.exit(0 if success else 1)

        applied, skipped, failed = runner.run_migrations()
        runner.close()

        logger.info(f"Migration summary: {applied} applied, {skipped} skipped, {failed} failed")

        if failed > 0:
            sys.exit(1)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
