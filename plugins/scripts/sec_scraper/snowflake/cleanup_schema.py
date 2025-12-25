#!/usr/bin/env python3
"""
Cleanup script to drop all objects in a Snowflake schema.

WARNING: This will delete all tables, views, and other objects in the schema!
Use with caution.

Usage:
    python cleanup_schema.py [--schema SCHEMA_NAME] [--dry-run]
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

try:
    import snowflake.connector
except ImportError:
    print("Error: snowflake-connector-python not installed.")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)

logger = logging.getLogger(__name__)


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


def cleanup_schema(conn, schema: str, dry_run: bool = False):
    """Drop all objects in the schema."""
    cursor = conn.cursor()

    try:
        # Get all views
        cursor.execute(f"SHOW VIEWS IN SCHEMA {schema}")
        views = cursor.fetchall()
        view_names = [v[1] for v in views]  # View name is at index 1

        # Get all tables
        cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
        tables = cursor.fetchall()
        table_names = [t[1] for t in tables]  # Table name is at index 1

        logger.info(f"Found {len(views)} views and {len(tables)} tables in schema {schema}")

        # Drop views first (they may depend on tables)
        for view_name in view_names:
            if dry_run:
                logger.info(f"DRY RUN: Would drop view {schema}.{view_name}")
            else:
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS {schema}.{view_name}")
                    logger.info(f"✓ Dropped view {schema}.{view_name}")
                except Exception as e:
                    logger.error(f"✗ Failed to drop view {schema}.{view_name}: {e}")

        # Drop tables
        for table_name in table_names:
            if dry_run:
                logger.info(f"DRY RUN: Would drop table {schema}.{table_name}")
            else:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
                    logger.info(f"✓ Dropped table {schema}.{table_name}")
                except Exception as e:
                    logger.error(f"✗ Failed to drop table {schema}.{table_name}: {e}")

        # Optionally drop the schema itself
        if not dry_run and (len(views) > 0 or len(tables) > 0):
            logger.info(f"Note: Schema {schema} still exists. Drop it manually if needed: DROP SCHEMA IF EXISTS {schema}")

    finally:
        cursor.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Cleanup (drop all objects) in a Snowflake schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
WARNING: This will delete ALL tables and views in the specified schema!

Example:
    python cleanup_schema.py --schema sec_raw
    python cleanup_schema.py --schema sec_raw --dry-run
        """
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

    # Confirm (unless dry-run)
    if not args.dry_run:
        response = input(f"WARNING: This will delete ALL objects in schema {args.schema}. Continue? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Aborted.")
            sys.exit(0)

    # Connect and cleanup
    try:
        conn = snowflake.connector.connect(
            account=config["account"],
            user=config["user"],
            password=config["password"],
            warehouse=config["warehouse"],
            database=config["database"],
            schema=config["schema"],
            role=config.get("role"),
        )
        logger.info(f"Connected to Snowflake: {config['account']}/{config['database']}/{config['schema']}")

        cleanup_schema(conn, config["schema"], args.dry_run)

        logger.info("✓ Cleanup complete")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()

