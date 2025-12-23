#!/usr/bin/env python3
"""
Deploy SEC scraper schemas to Snowflake.

This script applies SQL schema files to Snowflake in the correct order.
It can be run standalone or from within Airflow.

Usage:
    python deploy_schemas.py [--dry-run] [--schema SCHEMA_NAME]

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
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

try:
    import snowflake.connector
except ImportError:
    print("Error: snowflake-connector-python not installed.")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)

logger = logging.getLogger(__name__)


class SnowflakeDeployer:
    """Deploy SQL schemas to Snowflake."""

    # Order matters - these files must be applied in this sequence
    SQL_FILES = [
        "submissions.sql",
        "companyfacts.sql",
        "us_gaap_metric_abbreviations.sql",
        "submissions_ticker_mapping.sql",
    ]

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
            logger.info(f"Connected to Snowflake: {self.account}/{self.database}/{self.schema}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def close(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")

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

    def deploy(self, sql_dir: Path):
        """Deploy all SQL files in order."""
        logger.info(f"Deploying schemas to Snowflake schema: {self.schema}")

        # Ensure schema exists
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
        self.execute_sql(create_schema_sql, f"Create schema {self.schema}")

        # Deploy SQL files in order
        for sql_file in self.SQL_FILES:
            filepath = sql_dir / sql_file
            if not filepath.exists():
                logger.warning(f"SQL file not found: {filepath}")
                continue

            logger.info(f"Processing: {sql_file}")
            sql = self.read_sql_file(filepath)

            # Split SQL file by semicolons (handling multi-statement files)
            statements = self._split_sql_statements(sql)

            for i, statement in enumerate(statements, 1):
                statement = statement.strip()
                if not statement or statement.startswith("--"):
                    continue

                description = f"{sql_file} (statement {i}/{len(statements)})"
                self.execute_sql(statement, description)

        logger.info("✓ Schema deployment complete")

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
    parser = argparse.ArgumentParser(description="Deploy SEC scraper schemas to Snowflake")
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path(__file__).parent.parent / "schemas" / "sec_raw",
        help="Directory containing SQL files (default: ../schemas/sec_raw)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to JSON config file (optional, env vars override)",
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

    # Check SQL directory
    if not args.sql_dir.exists():
        logger.error(f"SQL directory not found: {args.sql_dir}")
        sys.exit(1)

    # Deploy
    deployer = SnowflakeDeployer(
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
        deployer.connect()
        deployer.deploy(args.sql_dir)
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)
    finally:
        deployer.close()


if __name__ == "__main__":
    main()

