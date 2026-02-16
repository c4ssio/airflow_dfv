from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Set

from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

try:
    import psycopg2
    import yaml
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    yaml = None  # type: ignore


def get_postgres_config(config_path: str) -> Dict[str, Any]:
    """Load PostgreSQL configuration from env vars (preferred) or YAML file.

    When POSTGRES_HOST is set (e.g. via AWS Secrets Manager), env vars take
    precedence and the YAML file is not required.
    """
    # Env-var path: AWS Secrets Manager / ECS task env
    host = os.environ.get("POSTGRES_HOST", "").strip()
    if host:
        return {
            "host": host,
            "port": int(os.environ.get("POSTGRES_PORT", "5432")),
            "database": os.environ.get("POSTGRES_DB", "sec_data"),
            "user": os.environ.get("POSTGRES_USER", "airflow"),
            "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
            "schema": os.environ.get("POSTGRES_SCHEMA", "sec_raw"),
        }

    # YAML-file path: local Docker Compose setup
    if not yaml:
        raise RuntimeError(
            "pyyaml is required for PostgreSQL ingestion. Install with: pip install pyyaml"
        )

    config_file = Path(config_path)
    if not config_file.exists():
        raise RuntimeError(f"PostgreSQL config file not found: {config_path}")

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    required_keys = ["host", "port", "database", "user", "password"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise RuntimeError(f"Missing required PostgreSQL config keys: {', '.join(missing)}")

    return config


def write_ndjson_file(file_path: str, rows: List[Dict[str, Any]]) -> None:
    """Write rows to a newline-delimited JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def get_postgres_connection(config: Dict[str, Any], schema: str = "sec_raw") -> Any:
    """Get a connection to PostgreSQL."""
    if not psycopg2:
        raise RuntimeError("psycopg2 is required. Install with: pip install psycopg2-binary")

    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"],
        connect_timeout=30,
    )

    cursor = conn.cursor()
    cursor.execute(f"SET search_path TO {schema}, public")
    cursor.execute("SET statement_timeout = '300s'")
    cursor.close()
    conn.commit()

    return conn


def get_ciks_already_ingested(conn: Any, schema: str, ingest_date: str) -> Set[str]:
    """Return set of zero-padded CIKs that already have a row in submissions for this ingest_date."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT cik FROM {schema}.submissions WHERE ingest_date = %s",
            (ingest_date,),
        )
        return {row[0] for row in cursor.fetchall() if row and row[0]}
    finally:
        cursor.close()


def load_ndjson_batch_to_postgres(
    conn: Any,
    table_name: str,
    ndjson_paths: List[str],
    cik_list: List[str],
    ingest_date: str,
    schema: str = "sec_raw",
) -> int:
    """Load a batch of NDJSON files into PostgreSQL table using streaming INSERT."""
    if not ndjson_paths:
        return 0

    total_rows_loaded = 0
    files_processed = 0
    total_files = len(ndjson_paths)

    for path in ndjson_paths:
        if not os.path.exists(path):
            logger.warning("NDJSON file not found, skipping: %s", path)
            continue

        cik_dir_name = os.path.basename(os.path.dirname(path))
        cik = cik_dir_name.replace("cik=", "").lstrip("0") or "0"
        cik_padded = cik.zfill(10)

        cursor = conn.cursor()
        file_rows = 0

        try:
            delete_sql = f"DELETE FROM {schema}.{table_name} WHERE ingest_date = %s AND cik = %s"
            cursor.execute(delete_sql, [ingest_date, cik_padded])

            insert_sql = None
            columns = None
            batch_values = []
            batch_size = 1000

            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    row = json.loads(line)

                    if insert_sql is None:
                        columns = list(row.keys())
                        col_names = ", ".join(columns)
                        placeholders = ", ".join(["%s"] * len(columns))
                        insert_sql = (
                            f"INSERT INTO {schema}.{table_name} ({col_names}) "
                            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                        )

                    values = []
                    for col in columns:
                        val = row.get(col)
                        if isinstance(val, (dict, list)):
                            values.append(json.dumps(val))
                        else:
                            values.append(val)
                    batch_values.append(tuple(values))
                    file_rows += 1

                    if len(batch_values) >= batch_size:
                        cursor.executemany(insert_sql, batch_values)
                        batch_values = []

            if batch_values and insert_sql:
                cursor.executemany(insert_sql, batch_values)

            conn.commit()
            total_rows_loaded += file_rows
            files_processed += 1

            if files_processed % 10 == 0 or file_rows > 10000:
                logger.info(
                    "[%s] Progress: %d/%d files, %d total rows (CIK %s: %d rows)",
                    table_name,
                    files_processed,
                    total_files,
                    total_rows_loaded,
                    cik,
                    file_rows,
                )

        except Exception as e:
            conn.rollback()
            logger.error("Failed to load %s into %s.%s: %s", path, schema, table_name, e)
            continue
        finally:
            cursor.close()

    logger.info(
        "Successfully loaded %d rows into %s.%s from %d/%d files",
        total_rows_loaded,
        schema,
        table_name,
        files_processed,
        total_files,
    )
    return total_rows_loaded


def upsert_metric_metadata(
    conn: Any,
    metric_metadata_rows: List[Dict[str, Any]],
    schema: str = "sec_raw",
) -> int:
    """Upsert metric metadata (taxonomy, metric_name, label, description)."""
    if not metric_metadata_rows:
        return 0

    cursor = conn.cursor()
    upserted = 0

    try:
        upsert_sql = f"""
            INSERT INTO {schema}.metric_metadata (taxonomy, metric_name, label, description, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (taxonomy, metric_name) DO UPDATE SET
                label = COALESCE(EXCLUDED.label, {schema}.metric_metadata.label),
                description = COALESCE(EXCLUDED.description, {schema}.metric_metadata.description),
                updated_at = CURRENT_TIMESTAMP
        """

        for row in metric_metadata_rows:
            cursor.execute(
                upsert_sql,
                (
                    row.get("taxonomy"),
                    row.get("metric_name"),
                    row.get("label"),
                    row.get("description"),
                ),
            )
            upserted += 1

        conn.commit()
        return upserted

    except Exception as e:
        conn.rollback()
        logger.error("Failed to upsert metric metadata: %s", e)
        raise
    finally:
        cursor.close()
