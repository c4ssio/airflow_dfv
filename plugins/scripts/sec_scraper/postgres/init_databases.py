#!/usr/bin/env python3
"""
Initialize databases for SEC scraper on a fresh PostgreSQL instance (e.g. RDS).

This replaces the Docker-only ``init-sec-db.sql`` approach:
  1. Connects to the default ``postgres`` database.
  2. Creates the ``sec_data`` database if it doesn't exist.
  3. Runs all pending migrations via ``deploy_migrations.py``.

Usage (inside ECS init task or locally):
    python -m scripts.sec_scraper.postgres.init_databases [--verbose]

Configuration is read from POSTGRES_HOST / POSTGRES_* env vars (preferred)
or from ``postgres.yaml`` (fallback).
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Any, Dict

from scripts.sec_scraper.postgres.deploy_migrations import (
    MigrationRunner,
    ensure_database_exists,
    get_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def init_databases(config: Dict[str, Any], *, verbose: bool = False) -> None:
    """Ensure database + schema exist and run all pending migrations."""
    # Step 1: create the target database if missing
    logger.info("Ensuring database '%s' exists …", config.get("database", "sec_data"))
    ensure_database_exists(config)

    # Step 2: run migrations
    logger.info("Running migrations …")
    runner = MigrationRunner(config=config, verbose=verbose)
    try:
        applied, skipped, failed = runner.run_migrations()
        logger.info("Migrations: %d applied, %d skipped, %d failed", applied, skipped, failed)
        if failed:
            raise RuntimeError(f"{failed} migration(s) failed")
    finally:
        runner.close()

    logger.info("Database initialization complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize SEC scraper databases")
    parser.add_argument("--config", help="Path to postgres.yaml config file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    try:
        config = get_config(args.config)
        init_databases(config, verbose=args.verbose)
    except Exception as e:
        logger.error("Initialization failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
