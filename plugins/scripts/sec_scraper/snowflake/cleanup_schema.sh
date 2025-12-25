#!/bin/bash
# Cleanup script to drop all objects in a Snowflake schema
# WARNING: This will delete ALL tables and views in the schema!

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"

cd "$PROJECT_ROOT"

# Use the venv if it exists
if [ -d ".venv" ]; then
    .venv/bin/python "$SCRIPT_DIR/cleanup_schema.py" "$@"
else
    python3 "$SCRIPT_DIR/cleanup_schema.py" "$@"
fi

